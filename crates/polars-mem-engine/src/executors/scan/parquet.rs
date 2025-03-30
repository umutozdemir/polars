use hive::HivePartitions;
use polars_core::config;
#[cfg(feature = "cloud")]
use polars_core::config::{get_file_prefetch_size, verbose};
use polars_core::utils::accumulate_dataframes_vertical;
use polars_error::feature_gated;
use polars_io::RowIndex;
use polars_io::cloud::CloudOptions;
use polars_io::parquet::metadata::FileMetadataRef;
use polars_io::predicates::{ScanIOPredicate, SkipBatchPredicate};
use polars_io::utils::slice::split_slice_at_file;
use polars_compute::rolling::QuantileMethod;
use crate::executors::scan::sma::{OutlierEntry, SMAManager};
use super::*;
use crate::ScanPredicate;

pub struct ParquetExec {
    sources: ScanSources,
    file_info: FileInfo,

    hive_parts: Option<Arc<Vec<HivePartitions>>>,

    predicate: Option<ScanPredicate>,
    skip_batch_predicate: Option<Arc<dyn SkipBatchPredicate>>,

    pub(crate) options: ParquetOptions,
    #[allow(dead_code)]
    cloud_options: Option<CloudOptions>,
    file_options: Box<FileScanOptions>,
    #[allow(dead_code)]
    metadata: Option<FileMetadataRef>,
    sma_manager: SMAManager,
}

impl ParquetExec {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        sources: ScanSources,
        file_info: FileInfo,
        hive_parts: Option<Arc<Vec<HivePartitions>>>,
        predicate: Option<ScanPredicate>,
        options: ParquetOptions,
        cloud_options: Option<CloudOptions>,
        file_options: Box<FileScanOptions>,
        metadata: Option<FileMetadataRef>,
        // row_groups_to_read: Option<Vec<usize>>,
    ) -> Self {
        ParquetExec {
            sources,
            file_info,

            hive_parts,

            predicate,
            skip_batch_predicate: None,

            options,
            cloud_options,
            file_options,
            metadata,
            sma_manager: SMAManager::new(),
        }
    }

    fn read_par(&mut self) -> PolarsResult<Vec<DataFrame>> {
        let parallel = match self.options.parallel {
            ParallelStrategy::Auto if self.sources.len() > POOL.current_num_threads() => {
                ParallelStrategy::RowGroups
            },
            identity => identity,
        };

        let mut result = vec![];

        let step = std::cmp::min(POOL.current_num_threads(), 128);
        // Modified if we have a negative slice
        let mut first_source = 0;

        let first_schema = self.file_info.reader_schema.clone().unwrap().unwrap_left();

        let projected_arrow_schema = {
            if let Some(with_columns) = self.file_options.with_columns.as_deref() {
                Some(Arc::new(first_schema.try_project(with_columns)?))
            } else {
                None
            }
        };
        let predicate = self.predicate.as_ref().map(|p| {
            p.to_io(
                self.skip_batch_predicate.as_ref(),
                self.file_info.schema.clone(),
            )
        });
        let mut base_row_index = self.file_options.row_index.take();

        // (offset, end)
        let (slice_offset, slice_end) = if let Some(slice) = self.file_options.pre_slice {
            if slice.0 >= 0 {
                (slice.0 as usize, slice.1.saturating_add(slice.0 as usize))
            } else {
                // Walk the files in reverse until we find the first file, and then translate the
                // slice into a positive-offset equivalent.
                let slice_start_as_n_from_end = -slice.0 as usize;
                let mut cum_rows = 0;
                let mut first_source_row_offset = 0;
                let chunk_size = 8;
                POOL.install(|| {
                    for path_indexes in (0..self.sources.len())
                        .rev()
                        .collect::<Vec<_>>()
                        .chunks(chunk_size)
                    {
                        let row_counts = path_indexes
                            .into_par_iter()
                            .map(|&i| {
                                let memslice = self.sources.at(i).to_memslice()?;

                                let mut reader = ParquetReader::new(std::io::Cursor::new(memslice));

                                if i == 0 {
                                    if let Some(md) = self.metadata.clone() {
                                        reader.set_metadata(md)
                                    }
                                }

                                reader.num_rows()
                            })
                            .collect::<PolarsResult<Vec<_>>>()?;

                        for (path_idx, rc) in path_indexes.iter().zip(row_counts) {
                            if first_source == 0 {
                                cum_rows += rc;

                                if cum_rows >= slice_start_as_n_from_end {
                                    first_source = *path_idx;

                                    if base_row_index.is_none() {
                                        break;
                                    }
                                }
                            } else {
                                first_source_row_offset += rc;
                            }
                        }
                    }

                    PolarsResult::Ok(())
                })?;

                let (start, len) = if slice_start_as_n_from_end > cum_rows {
                    // We need to trim the slice, e.g. SLICE[offset: -100, len: 75] on a file of 50
                    // rows should only give the first 25 rows.
                    let first_file_position = slice_start_as_n_from_end - cum_rows;
                    (0, slice.1.saturating_sub(first_file_position))
                } else {
                    (cum_rows - slice_start_as_n_from_end, slice.1)
                };

                let end = start.saturating_add(len);

                if let Some(ri) = base_row_index.as_mut() {
                    ri.offset += first_source_row_offset as IdxSize;
                }

                (start, end)
            }
        } else {
            (0, usize::MAX)
        };

        let mut current_offset = 0;
        // Limit no. of files at a time to prevent open file limits.

        for i in (first_source..self.sources.len()).step_by(step) {
            let end = std::cmp::min(i.saturating_add(step), self.sources.len());

            if current_offset >= slice_end && !result.is_empty() {
                return Ok(result);
            }

            // First initialize the readers, predicates and metadata.
            // This will be used to determine the slices. That way we can actually read all the
            // files in parallel even if we add row index columns or slices.
            let iter = (i..end).into_par_iter().map(|i| {
                let source = self.sources.at(i);
                let hive_partitions = self
                    .hive_parts
                    .as_ref()
                    .map(|x| x[i].materialize_partition_columns());

                let memslice = source.to_memslice()?;

                let mut reader = ParquetReader::new(std::io::Cursor::new(memslice));

                if i == 0 {
                    if let Some(md) = self.metadata.clone() {
                        reader.set_metadata(md)
                    }
                }

                let mut reader = reader
                    .read_parallel(parallel)
                    .set_low_memory(self.options.low_memory)
                    .use_statistics(self.options.use_statistics)
                    .set_rechunk(false)
                    .with_hive_partition_columns(hive_partitions)
                    .with_include_file_path(
                        self.file_options
                            .include_file_paths
                            .as_ref()
                            .map(|x| (x.clone(), Arc::from(source.to_include_path_name()))),
                    );
                    // .with_row_groups(self.row_groups.clone());

                reader.num_rows().map(|num_rows| (reader, num_rows))
            });

            // We do this in parallel because wide tables can take a long time deserializing metadata.
            let readers_and_metadata = POOL.install(|| iter.collect::<PolarsResult<Vec<_>>>())?;

            let current_offset_ref = &mut current_offset;
            let row_statistics = readers_and_metadata
                .iter()
                .map(|(_, num_rows)| {
                    let cum_rows = *current_offset_ref;
                    (
                        cum_rows,
                        split_slice_at_file(current_offset_ref, *num_rows, slice_offset, slice_end),
                    )
                })
                .collect::<Vec<_>>();

            let allow_missing_columns = self.file_options.allow_missing_columns;

            let out = POOL.install(|| {
                readers_and_metadata
                    .into_par_iter()
                    .zip(row_statistics.into_par_iter())
                    .map(|((reader, _), (cumulative_read, slice))| {
                        let row_index = base_row_index.as_ref().map(|rc| RowIndex {
                            name: rc.name.clone(),
                            offset: rc.offset + cumulative_read as IdxSize,
                        });

                        let df = reader
                            .with_slice(Some(slice))
                            .with_row_index(row_index)
                            .with_predicate(predicate.clone())
                            .with_arrow_schema_projection(
                                &first_schema,
                                projected_arrow_schema.as_deref(),
                                allow_missing_columns,
                            )?
                            .finish()?;

                        Ok(df)
                    })
                    .collect::<PolarsResult<Vec<_>>>()
            })?;

            if result.is_empty() {
                result = out;
            } else {
                result.extend_from_slice(&out)
            }
        }

        Ok(result)
    }

    #[cfg(feature = "cloud")]
    async fn read_async(&mut self) -> PolarsResult<Vec<DataFrame>> {
        use futures::{StreamExt, stream};
        use polars_io::pl_async;
        use polars_io::utils::slice::split_slice_at_file;

        let verbose = verbose();
        let paths = self.sources.into_paths().unwrap();
        let first_metadata = &self.metadata;
        let cloud_options = self.cloud_options.as_ref();

        let mut result = vec![];
        let batch_size = get_file_prefetch_size();

        if verbose {
            eprintln!("POLARS PREFETCH_SIZE: {}", batch_size)
        }

        let first_schema = self.file_info.reader_schema.clone().unwrap().unwrap_left();

        let projected_arrow_schema = {
            if let Some(with_columns) = self.file_options.with_columns.as_deref() {
                Some(Arc::new(first_schema.try_project(with_columns)?))
            } else {
                None
            }
        };
        let predicate = self.predicate.as_ref().map(|p| ScanIOPredicate {
            predicate: phys_expr_to_io_expr(p.predicate.clone()),
            live_columns: p.live_columns.clone(),
            skip_batch_predicate: self
                .skip_batch_predicate
                .clone()
                .or_else(|| p.to_dyn_skip_batch_predicate(self.file_info.schema.clone())),
            column_predicates: Arc::new(Default::default()),
        });
        let mut base_row_index = self.file_options.row_index.take();

        // Modified if we have a negative slice
        let mut first_file_idx = 0;

        // (offset, end)
        let (slice_offset, slice_end) = if let Some(slice) = self.file_options.pre_slice {
            if slice.0 >= 0 {
                (slice.0 as usize, slice.1.saturating_add(slice.0 as usize))
            } else {
                // Walk the files in reverse until we find the first file, and then translate the
                // slice into a positive-offset equivalent.
                let slice_start_as_n_from_end = -slice.0 as usize;
                let mut cum_rows = 0;
                let mut first_source_row_offset = 0;

                let paths = &paths;
                let cloud_options = Arc::new(self.cloud_options.clone());

                let paths = paths.clone();
                let cloud_options = cloud_options.clone();

                let mut iter = stream::iter((0..paths.len()).rev().map(|i| {
                    let paths = paths.clone();
                    let cloud_options = cloud_options.clone();
                    let first_metadata = first_metadata.clone();

                    pl_async::get_runtime().spawn(async move {
                        PolarsResult::Ok((
                            i,
                            ParquetAsyncReader::from_uri(
                                paths[i].to_str().unwrap(),
                                cloud_options.as_ref().as_ref(),
                                first_metadata.filter(|_| i == 0),
                            )
                            .await?
                            .num_rows()
                            .await?,
                        ))
                    })
                }))
                .buffered(8);

                while let Some(v) = iter.next().await {
                    let (path_idx, num_rows) = v.unwrap()?;

                    if first_file_idx == 0 {
                        cum_rows += num_rows;

                        if cum_rows >= slice_start_as_n_from_end {
                            first_file_idx = path_idx;

                            if base_row_index.is_none() {
                                break;
                            }
                        }
                    } else {
                        first_source_row_offset += num_rows;
                    }
                }

                let (start, len) = if slice_start_as_n_from_end > cum_rows {
                    // We need to trim the slice, e.g. SLICE[offset: -100, len: 75] on a file of 50
                    // rows should only give the first 25 rows.
                    let first_file_position = slice_start_as_n_from_end - cum_rows;
                    (0, slice.1.saturating_sub(first_file_position))
                } else {
                    (cum_rows - slice_start_as_n_from_end, slice.1)
                };

                let end = start.saturating_add(len);

                if let Some(ri) = base_row_index.as_mut() {
                    ri.offset += first_source_row_offset as IdxSize;
                }

                (start, end)
            }
        } else {
            (0, usize::MAX)
        };

        let mut current_offset = 0;
        let mut processed = 0;

        for batch_start in (first_file_idx..paths.len()).step_by(batch_size) {
            let end = std::cmp::min(batch_start.saturating_add(batch_size), paths.len());
            let paths = &paths[batch_start..end];
            let hive_parts = self.hive_parts.as_ref().map(|x| &x[batch_start..end]);

            if current_offset >= slice_end && !result.is_empty() {
                return Ok(result);
            }
            processed += paths.len();
            if verbose {
                eprintln!(
                    "querying metadata of {}/{} files...",
                    processed,
                    paths.len()
                );
            }

            // First initialize the readers and get the metadata concurrently.
            let iter = paths.iter().enumerate().map(|(i, path)| async move {
                let first_file = batch_start == 0 && i == 0;
                // use the cached one as this saves a cloud call
                let metadata = if first_file {
                    first_metadata.clone()
                } else {
                    None
                };
                let mut reader =
                    ParquetAsyncReader::from_uri(&path.to_string_lossy(), cloud_options, metadata)
                        .await?;

                let num_rows = reader.num_rows().await?;
                PolarsResult::Ok((num_rows, reader))
            });
            let readers_and_metadata = futures::future::try_join_all(iter).await?;

            let current_offset_ref = &mut current_offset;

            // Then compute `n_rows` to be taken per file up front, so we can actually read concurrently
            // after this.
            let row_statistics = readers_and_metadata
                .iter()
                .map(|(num_rows, _)| {
                    let cum_rows = *current_offset_ref;
                    (
                        cum_rows,
                        split_slice_at_file(current_offset_ref, *num_rows, slice_offset, slice_end),
                    )
                })
                .collect::<Vec<_>>();

            // Now read the actual data.
            let use_statistics = self.options.use_statistics;
            let base_row_index_ref = &base_row_index;
            let include_file_paths = self.file_options.include_file_paths.as_ref();
            let first_schema = first_schema.clone();
            let projected_arrow_schema = projected_arrow_schema.clone();
            let predicate = predicate.clone();
            let allow_missing_columns = self.file_options.allow_missing_columns;

            if verbose {
                eprintln!("reading of {}/{} file...", processed, paths.len());
            }

            let iter = readers_and_metadata
                .into_iter()
                .enumerate()
                .map(|(i, (_, reader))| {
                    let first_schema = first_schema.clone();
                    let projected_arrow_schema = projected_arrow_schema.clone();
                    let predicate = predicate.clone();
                    let (cumulative_read, slice) = row_statistics[i];
                    let hive_partitions = hive_parts
                        .as_ref()
                        .map(|x| x[i].materialize_partition_columns());

                    async move {
                        let row_index = base_row_index_ref.as_ref().map(|rc| RowIndex {
                            name: rc.name.clone(),
                            offset: rc.offset + cumulative_read as IdxSize,
                        });

                        let df = reader
                            .with_slice(Some(slice))
                            .with_row_index(row_index)
                            .with_arrow_schema_projection(
                                &first_schema,
                                projected_arrow_schema.as_deref(),
                                allow_missing_columns,
                            )
                            .await?
                            .use_statistics(use_statistics)
                            .with_predicate(predicate)
                            .set_rechunk(false)
                            .with_hive_partition_columns(hive_partitions)
                            .with_include_file_path(
                                include_file_paths
                                    .map(|x| (x.clone(), Arc::from(paths[i].to_str().unwrap()))),
                            )
                            .finish()
                            .await?;

                        PolarsResult::Ok(df)
                    }
                });

            let dfs = futures::future::try_join_all(iter).await?;
            result.extend(dfs.into_iter())
        }

        Ok(result)
    }

    fn read_impl(&mut self) -> PolarsResult<DataFrame> {
        // FIXME: The row index implementation is incorrect when a predicate is
        // applied. This code mitigates that by applying the predicate after the
        // collection of the entire dataframe if a row index is requested. This is
        // inefficient.

        let mut sma_file_exists: bool = false;
        let mut sma_entry_for_predicate_exists: bool = false;

        // I can make the use_sma check here and return empty result for example
        if self.options.use_sma {
            println!("use_sma is enabled");
            // paths is an array contains may contain multiple parquet file paths, but I assume there
            // will be always one parquet file in query.
            let paths = self.sources.into_paths().unwrap();
            let file_path = paths.get(0).unwrap().to_str().unwrap();

            let sma_file_name = file_path.replace(".parquet", ".sma");

            let mut predicate_column: Option<PlSmallStr> = None;

            if let Some(predicates) = self.predicate.clone() {
                predicate_column = predicates.live_columns.iter().next().cloned();
                println!("predicate_column: {:?}", predicate_column);
            } else {
                eprintln!("predicates is None");
            }

            sma_file_exists = self.sma_manager.can_retrieve_from_sma(sma_file_name.as_str());

            if sma_file_exists {
                println!("SMA entry for predicate exists");

                let sma = self.sma_manager.deserialize_sma_file(sma_file_name.as_str())?;

                if let Some(col_name) = predicate_column.as_ref() {
                    if sma.results.contains_key(col_name.as_str()) {
                        sma_entry_for_predicate_exists = true;
                        if let Some(result) = sma.results.get(col_name.as_str()) {
                            // TODO: Check if the filter satisfies outlier conditions
                            println!("Result found, returning from outliers");
                            return Ok(result.outliers.clone().unwrap_or_else(|| DataFrame::empty()));
                        } else {
                            println!("No result found for given predicate");
                        }
                    } else {
                        println!("SMA entry for given predicate does not exists");
                    }
                } else {
                    println!("No column name found for given predicate");
                }

                return Ok(DataFrame::empty());
            }
        }

        println!("use_sma is passed");

        let post_predicate = self
            .file_options
            .row_index
            .as_ref()
            .and_then(|_| self.predicate.take())
            .map(|p| phys_expr_to_io_expr(p.predicate));

        let is_cloud = self.sources.is_cloud_url();
        let force_async = config::force_async();

        let out = if is_cloud || (self.sources.is_paths() && force_async) {
            feature_gated!("cloud", {
                if force_async && config::verbose() {
                    eprintln!("ASYNC READING FORCED");
                }

                polars_io::pl_async::get_runtime().block_in_place_on(self.read_async())?
            })
        } else {
            self.read_par()?
        };

        let mut out = accumulate_dataframes_vertical(out)?;

        let num_unfiltered_rows = out.height();
        self.file_info.row_estimation = (Some(num_unfiltered_rows), num_unfiltered_rows);

        polars_io::predicates::apply_predicate(&mut out, post_predicate.as_deref(), true)?;

        if self.file_options.rechunk {
            out.as_single_chunk_par();
        }

        // Check if we need to create SMA from the given query.
        if self.options.use_sma {
            println!("use_sma is enabled-2");
            println!("SMA will be inserted/created");
            let column_name = if let Some(predicates) = self.predicate.clone() {
                predicates.live_columns.iter().next().cloned()
            } else {
                eprintln!("predicates is None");
                None
            };

            if let Some(ref col_name) = column_name {
                if let Some(column) = out.column(col_name.as_str()).ok() {
                    let series = column.as_series().unwrap();
                    let q1 = series.quantile_reduce(0.25, QuantileMethod::Linear)?;
                    let q3 = series.quantile_reduce(0.75, QuantileMethod::Linear)?;

                    let q1_float = match q1.value() {
                        AnyValue::Float64(v) => v,
                        _ => return Err(PolarsError::ComputeError("Expected Float64 value for Q1".into())),
                    };

                    let q3_float = match q3.value() {
                        AnyValue::Float64(v) => v,
                        _ => return Err(PolarsError::ComputeError("Expected Float64 value for Q3".into())),
                    };

                    let iqr = q3_float - q1_float;
                    let lower_threshold = q1_float - 1.5 * iqr;
                    let upper_threshold = q3_float + 1.5 * iqr;

                    println!("Lower Threshold: {}, Upper Threshold: {}", lower_threshold, upper_threshold);

                    let paths = self.sources.into_paths().unwrap();
                    let file_path = paths.get(0).unwrap().to_str().unwrap();
                    let sma_file_name = file_path.replace(".parquet", ".sma");

                    // get the column that lower&upper threshold applies
                    let column_series = out.column(col_name.clone().as_str())?;

                    // Create a Boolean mask for numeric datatypes
                    let lower_mask = match column_series.dtype() {
                        DataType::Float64 => {
                            let lower_threshold_f64 = lower_threshold as f64;
                            column_series.f64()?.lt(lower_threshold_f64)
                        },
                        DataType::Float32 => {
                            let lower_threshold_f32 = lower_threshold as f32;
                            column_series.f32()?.lt(lower_threshold_f32)
                        },
                        DataType::Int32 => {
                            let lower_threshold_i32 = lower_threshold as i32;
                            column_series.i32()?.lt(lower_threshold_i32)
                        },
                        DataType::Int64 => {
                            let lower_threshold_i64 = lower_threshold as i64;
                            column_series.i64()?.lt(lower_threshold_i64)
                        },
                        _ => return Err(PolarsError::ComputeError("Unsupported dtype for comparison".into())),
                    };

                    let upper_mask = match column_series.dtype() {
                        DataType::Float64 => {
                            let upper_threshold_f64 = upper_threshold as f64;
                            column_series.f64()?.gt(upper_threshold_f64)
                        },
                        DataType::Float32 => {
                            let upper_threshold_f32 = upper_threshold as f32;
                            column_series.f32()?.gt(upper_threshold_f32)
                        },
                        DataType::Int32 => {
                            let upper_threshold_i32 = upper_threshold as i32;
                            column_series.i32()?.gt(upper_threshold_i32)
                        },
                        DataType::Int64 => {
                            let upper_threshold_i64 = upper_threshold as i64;
                            column_series.i64()?.gt(upper_threshold_i64)
                        },
                        _ => return Err(PolarsError::ComputeError("Unsupported dtype for comparison".into())),
                    };
                    let outlier_mask = &lower_mask | &upper_mask;

                    // Apply the filter to get only outlier rows
                    let outliers_df = out.filter(&outlier_mask)?;
                    println!("Detected {} outliers based on thresholds.", outliers_df.height());
                    // TODO: It should have worked predicate based. a<50 or a>50 a>160
                    let sma_entry: OutlierEntry =  OutlierEntry {
                        predicate: col_name.clone().into_string(),
                        min: series.min()?.unwrap_or(f64::NEG_INFINITY),
                        max: series.max()?.unwrap_or(f64::INFINITY),
                        lower_threshold,
                        upper_threshold,
                        outliers: Some(outliers_df),
                    };

                    // First create the sma file if not exist
                    if !sma_file_exists {
                        println!("Creating SMA file");
                        self.sma_manager.create_sma_file(sma_file_name.as_ref())
                            .expect("File creation failed");
                        println!("Successfully created SMA file");
                    } else {
                        println!("SMA file already exists");
                    }

                    let col_name_str = column_name.as_ref().unwrap().to_string();

                    if !sma_entry_for_predicate_exists {
                        // Read the binary SMA file, add its results for the given predicate then save it back.
                        let mut sma = self.sma_manager.deserialize_sma_file(sma_file_name.as_str())?;
                        sma.results.insert(col_name_str, sma_entry);
                        self.sma_manager.update_sma_file(sma_file_name.as_str(), sma)
                            .expect("SMA file could not be updated");
                        println!("Successfully updated the SMA entry");
                    } else {
                        println!("SMA entry for given predicate already exists");
                    }

                } else {
                    println!("No column name found for given predicate");
                }
            } else {
                println!("No column name found for given predicate");
            }
        }

        Ok(out)
    }

    fn metadata_sync(&mut self) -> PolarsResult<&FileMetadataRef> {
        let memslice = self.sources.get(0).unwrap().to_memslice()?;
        Ok(self.metadata.insert(
            ParquetReader::new(std::io::Cursor::new(memslice))
                .get_metadata()?
                .clone(),
        ))
    }

    #[cfg(feature = "cloud")]
    async fn metadata_async(&mut self) -> PolarsResult<&FileMetadataRef> {
        let ScanSourceRef::Path(path) = self.sources.get(0).unwrap() else {
            unreachable!();
        };

        let mut reader =
            ParquetAsyncReader::from_uri(path.to_str().unwrap(), self.cloud_options.as_ref(), None)
                .await?;

        Ok(self.metadata.insert(reader.get_metadata().await?.clone()))
    }

    fn metadata(&mut self) -> PolarsResult<&FileMetadataRef> {
        let metadata = self.metadata.take();
        if let Some(md) = metadata {
            return Ok(self.metadata.insert(md));
        }

        #[cfg(feature = "cloud")]
        if self.sources.is_cloud_url() {
            return polars_io::pl_async::get_runtime().block_in_place_on(self.metadata_async());
        }

        self.metadata_sync()
    }
}

impl ScanExec for ParquetExec {
    fn read(
        &mut self,
        with_columns: Option<Arc<[PlSmallStr]>>,
        slice: Option<(usize, usize)>,
        predicate: Option<ScanPredicate>,
        skip_batch_predicate: Option<Arc<dyn SkipBatchPredicate>>,
        row_index: Option<RowIndex>,
    ) -> PolarsResult<DataFrame> {
        self.file_options.with_columns = with_columns;
        self.file_options.pre_slice = slice.map(|(o, l)| (o as i64, l));
        self.predicate = predicate;
        self.skip_batch_predicate = skip_batch_predicate;
        self.file_options.row_index = row_index;

        if self.file_info.reader_schema.is_none() {
            self.schema()?;
        }
        self.read_impl()
    }

    fn schema(&mut self) -> PolarsResult<&SchemaRef> {
        if self.file_info.reader_schema.is_some() {
            return Ok(&self.file_info.schema);
        }

        let md = self.metadata()?;
        let arrow_schema = polars_io::parquet::read::infer_schema(md)?;
        self.file_info.schema =
            Arc::new(Schema::from_iter(arrow_schema.iter().map(
                |(name, field)| (name.clone(), DataType::from_arrow_field(field)),
            )));
        self.file_info.reader_schema = Some(arrow::Either::Left(Arc::new(arrow_schema)));

        Ok(&self.file_info.schema)
    }

    fn num_unfiltered_rows(&mut self) -> PolarsResult<IdxSize> {
        let md = self.metadata()?;
        Ok(md.num_rows as IdxSize)
    }
}

impl Executor for ParquetExec {
    fn execute(&mut self, state: &mut ExecutionState) -> PolarsResult<DataFrame> {
        let profile_name = if state.has_node_timer() {
            let mut ids = vec![self.sources.id()];
            if self.predicate.is_some() {
                ids.push("predicate".into())
            }
            let name = comma_delimited("parquet".to_string(), &ids);
            Cow::Owned(name)
        } else {
            Cow::Borrowed("")
        };

        state.record(|| self.read_impl(), profile_name)
    }
}
