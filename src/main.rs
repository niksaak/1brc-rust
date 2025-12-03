#![allow(unused_imports)]

use std::{
    io::{prelude::*, Cursor},
    fs::File,
    collections::BTreeMap,
};

use rayon::prelude::*;
use memmap2::Mmap;
use tracing_subscriber::prelude::*;

#[tracing::instrument(skip_all)]
fn main() -> Result<(), eyre::Report> {
    if let Err(e) = dotenvy::dotenv() {
        eprintln!("dotenv: {e}");
    }

    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_error::ErrorLayer::default())
        .try_init()?;

    color_eyre::install()?;

    run()
}

#[derive(Clone, Copy, Debug)]
struct CumState {
    min: f32,
    avg: f32,
    max: f32,
    count: u32,
}

fn run() -> Result<(), eyre::Report> {
    let file = File::open("measurements.txt")?;
    let measurements_bytes = unsafe { Mmap::map(&file) }?;

    rayon::ThreadPoolBuilder::new().num_threads(8).build_global()?;

    let map = measurements_bytes.par_split(|c| *c == b'\n')
        .filter_map(|subslice| {
            let idx = subslice.into_iter().position(|c| *c == b';')?;
            let (place, reading) = subslice.split_at(idx);
            let reading = &reading[1..];
            let place = str::from_utf8(place).ok()?;
            let reading: f32 = str::from_utf8(reading).ok()?.parse().ok()?;
            Some((place, reading))
        })

        .fold(
            || <BTreeMap<&str, CumState>>::default(),
            |mut map, (place, reading)| {
                map.entry(place)
                    .and_modify(|CumState{ min, avg, max, count }| {
                        *min = min.min(reading);
                        *max = max.max(reading);
                        *avg = {
                            let avg = *avg;
                            let count = *count as f32;
                            (reading + avg * count) / (count + 1.0)
                        };
                        *count += 1;
                    })
                    .or_insert(CumState{ min: reading, avg: reading, max: reading, count: 1 });
                map
            }
        )

        .reduce(
            || <BTreeMap<&str, CumState>>::default(),
            |mut base, other| {
                for (place, st) in other {
                    base.entry(place)
                        .and_modify(|CumState{ min, avg, max, count }| {
                            *min = min.min(st.min);
                            *max = max.max(st.max);
                            *avg = {
                                let avg = *avg;
                                let count = *count as f32;
                                let st_count = st.count as f32;

                                (st.avg * st_count + avg * count) / (st_count + count)
                            };
                            *count += st.count;
                        })
                        .or_insert(st);
                }

                base
            }
        );

    /*
    // single threaded version
    let map = <BTreeMap<&str, CumState>>::default();

    for (place, reading) in measurements_bytes.split(|c| *c == b'\n') {
        map.entry(place)
            .and_modify(|CumState{ min, avg, max, count }| {
                *min = min.min(reading);
                *max = max.max(reading);
                *avg = {
                    let avg = *avg;
                    let count = *count as f32;
                    (reading + avg * count) / (count + 1.0)
                };
                *count += 1;
            })
            .or_insert(CumState{
                min: reading,
                avg: reading,
                max: reading,
                count: 1
            });
    }
    */

    for (place, CumState{ min, avg, max, ..}) in map {
        println!("{place}={min:.1}/{avg:.1}/{max:.1}");
    }

    Ok(())
}

