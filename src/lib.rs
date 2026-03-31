pub mod grpc {
    include!("./generated/coordinator.v1.rs");
}
use futures_util::{StreamExt, TryStreamExt};
use jsonwebtoken::{encode, EncodingKey, Header};
use serde::Serialize;

use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};

use futures_core::stream::BoxStream;
use grpc::{
    browse_dataset_response::{BrowsePhase, Event},
    browse_error::ErrorCode,
    dataset_service_server::DatasetService,
    BrowseComplete, BrowseDatasetRequest, BrowseDatasetResponse, BrowseError, BrowseProgress,
    DatasetInfo, FileEntry,
};

use prost_types::Timestamp;
use serde::Deserialize;
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use url::Url;

use crate::grpc::{
    dataplayer_service_server::DataplayerService, get_artifact_response::EntryPoint,
    tool_service_server::ToolService, tool_status, BrowseDatasetByUrlRequest, BrowseToolsRequest,
    BrowseToolsResponse, DropRequest, DropResponse, EoscInlineTool, FindToolsRequest,
    FindToolsResponse, GetArtifactRequest, GetArtifactResponse, GetStatusRequest,
    GetStatusResponse, GetToolRequest, HostedTool, LaunchToolRequest, LaunchToolResponse,
    MonitorStatusRequest, MonitorStatusResponse, QueryUserRequest, QueryUserResponse, ToolHandler,
    ToolMeta, ToolResponse,
};

fn current_timestamp() -> Timestamp {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");
    Timestamp {
        seconds: now.as_secs().cast_signed(),
        nanos: now.subsec_nanos().cast_signed(),
    }
}

#[async_trait::async_trait]
pub trait DataSource: Send + Sync + 'static {
    // get dataset information
    async fn get_dataset_info(&self, uuid: &str) -> anyhow::Result<DatasetInfo>;
    /// list files in the dataset
    /// # Errors
    /// ???
    async fn list_files(&self, uuid: &str) -> anyhow::Result<BoxStream<'static, FileEntry>>;
}

#[derive(Debug)]
struct Dataset {
    // XXX: I don't want to couple the grpc logic with business logic, so I need real type for both
    // datasetinfo and fileentry.
    info: DatasetInfo,
    files: Vec<FileEntry>,
}

// TODO: rename to DataRepositoryProxy??
// This play the role to relay the API calls to source data repository through filemetrix service.
pub struct DataRelayer {
    // TODO: source of tool-registry, mocked by a JSON, in production can be just tool-registry
    // API call address.
    // TODO: source of type-registry, mocked by a JSON
    // TODO: source of data repositories, mocked by a sqlite, the arch here not clear, should this
    // all behind the filemetrix? Or get from filemetrix (seems better because I don't want RP
    // tangled directly with DB, it is good to have operations behind filemetrix and this is one of
    // the roles filemetrix need to play) the basic info and query from DB after?
    data_source: Arc<dyn DataSource>,
}

impl DataRelayer {
    pub fn new(src: Arc<dyn DataSource>) -> Self {
        Self { data_source: src }
    }
}

// XXX: the logic and transport mixed here, I need to have a DatasetBrowser for the inner browse
// logic, then I can do the same no matter for filemetrix, or self directy service, or mocked test.
#[allow(clippy::too_many_lines)]
#[tonic::async_trait]
impl DatasetService for DataRelayer {
    type BrowseDatasetStream = ReceiverStream<Result<BrowseDatasetResponse, Status>>;
    type BrowseDatasetByUrlStream = ReceiverStream<Result<BrowseDatasetResponse, Status>>;

    async fn browse_dataset_by_url(
        &self,
        request: Request<BrowseDatasetByUrlRequest>,
    ) -> Result<Response<Self::BrowseDatasetByUrlStream>, Status> {
        // TODO: tracing
        println!("Got a request: {request:?}");
        let (tx, rx) = mpsc::channel(16);
        let data_source = Arc::clone(&self.data_source);

        tokio::spawn(async move {
            // INIT Phase
            let req = request.get_ref();
            let url = &req.url;

            // TODO:
            // NOTE: datasets are with versions
            // while files are with modified/updated timestamps.
            let dataset_info = match data_source.get_dataset_info(url).await {
                Ok(info) => info,
                Err(err) => {
                    let err = BrowseError {
                        code: ErrorCode::UnavailableFilemetrix as i32,
                        message: format!("unable to get dataset info of url: {url}, because of filemetrix error: {err}"),
                        path: None,
                        fatal: true,
                    };
                    tx.send(Ok(BrowseDatasetResponse {
                        phase: BrowsePhase::PhaseInit as i32,
                        event: Some(Event::Error(err)),
                    }))
                    .await
                    .ok();

                    return;
                }
            };
            tx.send(Ok(BrowseDatasetResponse {
                phase: BrowsePhase::PhaseInit as i32,
                event: Some(Event::DatasetInfo(dataset_info.clone())),
            }))
            .await
            .ok();

            tx.send(Ok(BrowseDatasetResponse {
                phase: BrowsePhase::PhaseBrowsing as i32,
                event: Some(Event::Progress(BrowseProgress {
                    files_scanned: 0,
                    bytes_scanned: 0,
                    percent: 0,
                    path: None,
                })),
            }))
            .await
            .ok();

            // Browsing, keep on sending file info of the dataset asynchronously
            let files = match data_source.list_files(url).await {
                Ok(files) => files,
                Err(err) => {
                    tracing::error!("cannot resolve files from url: {url}");
                    let err = BrowseError {
                        code: ErrorCode::UnavailableFilemetrix as i32,
                        message: format!(
                            "unable to list files from url: {url}, because of filemetrix error: {err}"
                        ),
                        path: None,
                        fatal: true,
                    };
                    tx.send(Ok(BrowseDatasetResponse {
                        phase: BrowsePhase::PhaseInit as i32,
                        event: Some(Event::Error(err)),
                    }))
                    .await
                    .ok();

                    return;
                }
            };

            let files_count = Arc::new(AtomicU64::new(0));
            let bytes_count = Arc::new(AtomicU64::new(0));
            // TODO: I may want to have pagination to at most showing 100 entries by default.
            // I need then have sever wait for incomming message to continue, bilateral required
            // and input needs to be a stream.
            files.for_each_concurrent(10, |file| {
                // dbg!(files_count);
                let tx = tx.clone();
                let dataset_info = dataset_info.clone();
                let files_count = files_count.clone();
                let bytes_count = bytes_count.clone();
                async move {
                    let filepath = file.path.clone();
                    let sizebytes = file.size_bytes;
                    if let Err(err) = tx
                        .send(Ok(BrowseDatasetResponse {
                            phase: BrowsePhase::PhaseBrowsing as i32,
                            event: Some(Event::FileEntry(file.clone())),
                        }))
                        .await
                    {
                        // Err
                        let err = BrowseError {
                            code: ErrorCode::UnavailableFile as i32,
                            message: format!("unable to send file: {url} file: {filepath} to client, because of: {err}"),
                            path: None,
                            fatal: true,
                        };
                        tx.send(Ok(BrowseDatasetResponse {
                            phase: BrowsePhase::PhaseInit as i32,
                            event: Some(Event::Error(err)),
                        }))
                        .await
                        .ok();
                    } else {
                        // Ok
                        let (new_files, new_bytes) = if !file.is_dir {
                            let new_files = files_count.fetch_add(1, Ordering::Relaxed) + 1;

                            let new_bytes =
                                bytes_count.fetch_add(sizebytes, Ordering::Relaxed)
                                + sizebytes;
                            (new_files, new_bytes)
                        } else {
                            (0,0)
                        };
                        tx.send(Ok(BrowseDatasetResponse {
                            phase: BrowsePhase::PhaseBrowsing as i32,
                            event: Some(Event::Progress(BrowseProgress {
                                files_scanned: new_files,
                                bytes_scanned: new_bytes,
                                // FIXME: don't calculate percent in server side, because the
                                // respond arrive in client side without orders.
                                #[allow(clippy::cast_possible_truncation)]
                                percent: ((new_files as f64
                                    / dataset_info.total_files() as f64) * 100.0) as u32,
                                path: None,
                        })),
                        }))
                        .await
                        .ok();
                    };

                    // TODO: further operations include:
                    // 1. file download, provide here? yes and calling scanning for mime-type and
                    //    checksum automatically if the file is small (this rely on the file size must
                    //    know beforehead).
                    // 3. mime type deduct?? should this purely be the responsibility of filemetrix??
                    //    (yes here)
                    // 2. relay file to the VREs? in a separated step? (in the seprated step)
                }
            }).await;

            let files_count = files_count.load(Ordering::Relaxed);
            let bytes_count = bytes_count.load(Ordering::Relaxed);
            let success = files_count == dataset_info.total_files()
                && bytes_count == dataset_info.total_size_bytes();

            tx.send(Ok(BrowseDatasetResponse {
                phase: BrowsePhase::PhaseCompleted as i32,
                event: Some(Event::Complete(BrowseComplete {
                    total_files: files_count,
                    total_size_bytes: bytes_count,
                    success,
                    finish_at: Some(current_timestamp()),
                })),
            }))
            .await
            .ok();
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    /// browse dataset through filemetrix API calls.
    /// XXX: I am expecting more than what filemetrix can provide.
    /// I mock those functionalities here and request filemetrix to have thoes implemneted.
    /// I need a service to downlead files for quick assessing (like a caching, caching <100k files).
    async fn browse_dataset(
        &self,
        request: Request<BrowseDatasetRequest>,
    ) -> Result<Response<Self::BrowseDatasetStream>, Status> {
        // TODO: tracing
        println!("Got a request: {request:?}");
        let (tx, rx) = mpsc::channel(16);
        let data_source = Arc::clone(&self.data_source);

        tokio::spawn(async move {
            // INIT Phase
            let req = request.get_ref();
            let uuid = &req.uuid;
            let url_datarepo = &req.url_datarepo;
            let id = &req.id_dataset;

            // TODO:
            // NOTE: datasets are with versions
            // while files are with modified/updated timestamps.
            let dataset_info = match data_source.get_dataset_info(uuid).await {
                Ok(info) => info,
                Err(err) => {
                    let err = BrowseError {
                        code: ErrorCode::UnavailableFilemetrix as i32,
                        message: format!("unable to get dataset info of url: {url_datarepo} - id: {id}, because of filemetrix error: {err}"),
                        path: None,
                        fatal: true,
                    };
                    tx.send(Ok(BrowseDatasetResponse {
                        phase: BrowsePhase::PhaseInit as i32,
                        event: Some(Event::Error(err)),
                    }))
                    .await
                    .ok();

                    return;
                }
            };
            tx.send(Ok(BrowseDatasetResponse {
                phase: BrowsePhase::PhaseInit as i32,
                event: Some(Event::DatasetInfo(dataset_info.clone())),
            }))
            .await
            .ok();

            tx.send(Ok(BrowseDatasetResponse {
                phase: BrowsePhase::PhaseBrowsing as i32,
                event: Some(Event::Progress(BrowseProgress {
                    files_scanned: 0,
                    bytes_scanned: 0,
                    percent: 0,
                    path: None,
                })),
            }))
            .await
            .ok();

            // Browsing, keep on sending file info of the dataset asynchronously
            let files = match data_source.list_files(uuid).await {
                Ok(files) => files,
                Err(err) => {
                    let err = BrowseError {
                        code: ErrorCode::UnavailableFilemetrix as i32,
                        message: format!("unable to list files url: {url_datarepo} - id: {id}, because of filemetrix error: {err}"),
                        path: None,
                        fatal: true,
                    };
                    tx.send(Ok(BrowseDatasetResponse {
                        phase: BrowsePhase::PhaseInit as i32,
                        event: Some(Event::Error(err)),
                    }))
                    .await
                    .ok();

                    return;
                }
            };

            let files_count = Arc::new(AtomicU64::new(0));
            let bytes_count = Arc::new(AtomicU64::new(0));
            // TODO: I may want to have pagination to at most showing 100 entries by default.
            // I need then have sever wait for incomming message to continue, bilateral required
            // and input needs to be a stream.
            files.for_each_concurrent(10, |file| {
                // dbg!(files_count);
                let tx = tx.clone();
                let dataset_info = dataset_info.clone();
                let files_count = files_count.clone();
                let bytes_count = bytes_count.clone();
                async move {
                    let filepath = file.path.clone();
                    let sizebytes = file.size_bytes;
                    if let Err(err) = tx
                        .send(Ok(BrowseDatasetResponse {
                            phase: BrowsePhase::PhaseBrowsing as i32,
                            event: Some(Event::FileEntry(file.clone())),
                        }))
                        .await
                    {
                        // Err
                        let err = BrowseError {
                            code: ErrorCode::UnavailableFile as i32,
                            message: format!("unable to send file: {url_datarepo} - id: {id} - file: {filepath} to client, because of: {err}"),
                            path: None,
                            fatal: true,
                        };
                        tx.send(Ok(BrowseDatasetResponse {
                            phase: BrowsePhase::PhaseInit as i32,
                            event: Some(Event::Error(err)),
                        }))
                        .await
                        .ok();
                    } else {
                        // Ok
                        let (new_files, new_bytes) = if !file.is_dir {
                            let new_files = files_count.fetch_add(1, Ordering::Relaxed) + 1;

                            let new_bytes =
                                bytes_count.fetch_add(sizebytes, Ordering::Relaxed)
                                + sizebytes;
                            (new_files, new_bytes)
                        } else {
                            (0,0)
                        };
                        tx.send(Ok(BrowseDatasetResponse {
                            phase: BrowsePhase::PhaseBrowsing as i32,
                            event: Some(Event::Progress(BrowseProgress {
                                files_scanned: new_files,
                                bytes_scanned: new_bytes,
                                // FIXME: don't calculate percent in server side, because the
                                // respond arrive in client side without orders.
                                #[allow(clippy::cast_possible_truncation)]
                                percent: ((new_files as f64
                                    / dataset_info.total_files() as f64) * 100.0) as u32,
                                path: None,
                        })),
                        }))
                        .await
                        .ok();
                    };

                    // TODO: further operations include:
                    // 1. file download, provide here? yes and calling scanning for mime-type and
                    //    checksum automatically if the file is small (this rely on the file size must
                    //    know beforehead).
                    // 3. mime type deduct?? should this purely be the responsibility of filemetrix??
                    //    (yes here)
                    // 2. relay file to the VREs? in a separated step? (in the seprated step)
                }
            }).await;

            let files_count = files_count.load(Ordering::Relaxed);
            let bytes_count = bytes_count.load(Ordering::Relaxed);
            let success = files_count == dataset_info.total_files()
                && bytes_count == dataset_info.total_size_bytes();

            tx.send(Ok(BrowseDatasetResponse {
                phase: BrowsePhase::PhaseCompleted as i32,
                event: Some(Event::Complete(BrowseComplete {
                    total_files: files_count,
                    total_size_bytes: bytes_count,
                    success,
                    finish_at: Some(current_timestamp()),
                })),
            }))
            .await
            .ok();
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

#[async_trait::async_trait]
pub trait ToolRegistryClient: Send + Sync + 'static {
    // get tool info by id
    async fn get_tool(&self, id: &str) -> anyhow::Result<VirtualResearchEnv>;
    // list tools in the registry, fine to return a Vec store in the ram can handle 10,000 entries.
    async fn list_tools(&self) -> anyhow::Result<Vec<VirtualResearchEnv>>;
}

// this is supposed to be the ro-crate that contain all information to launch the vre with required
// data pointers, so dispatcher or vre (depends on design of the dispatcher) can access the data
// without the needs to store data in the middleware.
// TODO: should not use tonic's FileEntry but a businiss faced own data structure.
pub struct LaunchRequset {
    // blob: Type
    id_vre: String,
    files: Vec<FileEntry>,
}

pub struct InfoRequest {}

#[async_trait::async_trait]
pub trait DispatcherClient: Send + Sync + 'static {
    // list all vre requests and their status
    async fn check_user_requests(&self, id_user: String) -> anyhow::Result<Vec<InfoRequest>>;
    // launch a vre with the launch request, return the callback url when it is ready
    async fn launch(&self, p: LaunchRequset) -> anyhow::Result<Url>;
}

pub struct RequestPackager {
    pub tool_registry: Arc<dyn ToolRegistryClient>,
    pub dispacher: Arc<dyn DispatcherClient>,
}

// assemble service happens after user select which vre to use and what files to attach with vre.
// The recommendation is happened before this service.
// Therefore, the request contains vre id selected and file entries selected.
// As return, it response the result that client side can use to directly open the tool.
// The response is *not* streamed back but a single solide resp contains the information on how to
// redirect to the launched (or directly launch for the inline tool case) vre.
//
// For vres that need to be launched through dispatcher, the request is blocking until the vre is
// ready. We use grpc so other rpc calls are not blocked.
// #[tonic::async_trait]
// impl AssembleService for RequestPackager {
//     // XXX: this rpc call may need to be separated into two calls, one use streams to get all
//     // information needed include resources whose necessity depends on the type of tools.
//     // Then send a whole pack and return resp after launch the vre.
//     async fn package_assemble(
//         &self,
//         mut request: Request<PackageAssembleRequest>,
//     ) -> Result<Response<PackageAssembleResponse>, Status> {
//         println!("Got a request: {request:?}");
//         let tool_registry = Arc::clone(&self.tool_registry);
//         let dispacher = Arc::clone(&self.dispacher);
//
//         // client (by user) says which tool to use and which files are selected to launch with vre
//         let req = request.get_mut();
//         let id_vre = &req.id_vre;
//         let files = &mut req.file_entries;
//
//         let tool = tool_registry.get_tool(id_vre).await.map_err(|e| {
//             // convert anyhow error to tonic status
//             println!("Failed to get tool from registry: {e:?}");
//             Status::internal(format!("Failed to get tool from registry: {e}"))
//         })?;
//
//         // TODO: assemble an ro-crate and send to dispatcher and get back the required vre callback
//         match tool {
//             VirtualResearchEnv::EoscInline { id, version } => {
//                 // check file number and simply relay (because I use same data structure for the
//                 // tool registry api call) the entry to the client
//
//                 // Inline tool only support passing one file, there might be use cases the tool
//                 // processes multiple files, but impl that when the case comes.
//                 if files.len() != 1 {
//                     let err_msg = format!(
//                         "inline tool only processes on one file, get: {}",
//                         files.len()
//                     );
//                     // TODO: proper tracing log
//                     println!("{err_msg}");
//                     return Err(Status::internal(err_msg));
//                 }
//
//                 // TODO: impl From trait to do the conversion
//                 // XXX: how inline tool get the file entry information? through payload? through
//                 // url query? or other machenism??
//                 let file = files.remove(0); // pop the file entry since I don't need it anymore
//
//                 // attach the file entry info and send back to client
//                 let vre = EntryPoint::EoscInline(VreEoscInline {
//                     url_callback: "https://example.com".to_string(),
//                     file_entry: Some(file),
//                 });
//                 let vre_entry = VreEntry {
//                     id_vre: id,
//                     version,
//                     entry_point: Some(vre),
//                 };
//
//                 // vre that not through dispatcher.
//                 let resp = PackageAssembleResponse {
//                     vre_entry: Some(vre_entry),
//                 };
//                 Ok(Response::new(resp))
//             }
//             VirtualResearchEnv::Hosted {
//                 id,
//                 version,
//                 requirements,
//             } => {
//                 // assamble a package and send to dispatcher that return a callback url
//                 // TODO: can check if the quota reached, users should not allowed to launch
//                 // infinit amount of vres (avoiding ddos).
//
//                 let filenames = files
//                     .iter()
//                     .map(|f| {
//                         let p = PathBuf::from(f.path.clone());
//                         // FIXME: dontpanic
//                         let p = p.file_name().and_then(|n| n.to_str()).unwrap().to_string();
//                         p
//                     })
//                     .collect::<Vec<String>>();
//
//                 if !requirements.iter().any(|r| filenames.contains(r)) {
//                     let err_msg = format!("{requirements:?} not fullfilled",);
//                     // TODO: proper tracing log
//                     println!("{err_msg}");
//                     return Err(Status::internal(err_msg));
//                 }
//
//                 // talk to dispatcher to launch a vre
//                 let launch_req = LaunchRequset {
//                     id_vre: id.clone(),
//                     files: files.clone(),
//                 };
//                 let url_callback = dispacher.launch(launch_req).await.map_err(|e| {
//                     // convert anyhow error to tonic status
//                     Status::internal(format!("dispacher launch failed because of {e}"))
//                 })?;
//                 let url_callback = url_callback.to_string();
//
//                 let vre = EntryPoint::Hosted(VreHosted { url_callback });
//                 let vre_entry = VreEntry {
//                     id_vre: id.clone(),
//                     version,
//                     entry_point: Some(vre),
//                 };
//
//                 // vre that not through dispatcher.
//                 let resp = PackageAssembleResponse {
//                     vre_entry: Some(vre_entry),
//                 };
//                 Ok(Response::new(resp))
//             }
//             _ => unimplemented!(),
//         }
//     }
// }
//
#[async_trait::async_trait]
pub trait ToolSource: Send + Sync + 'static {
    // // get dataset information
    // async fn get_dataset_info(&self, url_datarepo: &str, id: &str) -> anyhow::Result<DatasetInfo>;
    //
    // /// list files in the dataset
    // /// # Errors
    // /// ???
    // fn list_files(
    //     &self,
    //     url_datarepo: &str,
    //     id: &str,
    // ) -> anyhow::Result<BoxStream<'static, FileEntry>>;
    async fn find_tools(&self, files: &[FileEntry]) -> anyhow::Result<Vec<ToolMeta>>;
    async fn get_tool(&self, id: &str) -> anyhow::Result<ToolMeta>;
}

pub struct ToolDatabase {
    tool_source: Arc<dyn ToolSource>,
}

impl ToolDatabase {
    pub fn new(src: Arc<dyn ToolSource>) -> Self {
        Self { tool_source: src }
    }
}

#[tonic::async_trait]
impl ToolService for ToolDatabase {
    type BrowseToolsStream = ReceiverStream<Result<BrowseToolsResponse, Status>>;

    async fn get_tool(
        &self,
        request: Request<GetToolRequest>,
    ) -> Result<Response<ToolResponse>, Status> {
        todo!()
    }

    async fn find_tools(
        &self,
        req: Request<FindToolsRequest>,
    ) -> Result<Response<FindToolsResponse>, Status> {
        let req = req.get_ref();

        let tools = self
            .tool_source
            .find_tools(&req.files)
            .await
            // FIXME: Status::internal is too much, status code can granually deduct from API call errors, and setting
            // retry or report mechenism.
            .map_err(|err| Status::internal(format!("not find tool, {err}")))?;
        Ok(Response::new(FindToolsResponse { tools }))
    }

    async fn browse_tools(
        &self,
        request: Request<BrowseToolsRequest>,
    ) -> Result<Response<Self::BrowseToolsStream>, Status> {
        todo!()
    }
}

pub enum ToolStatus {
    Preparing,
    Ready,
    Dropped,
}

impl From<ToolStatus> for grpc::ToolStatus {
    fn from(status: ToolStatus) -> Self {
        match status {
            ToolStatus::Ready => grpc::ToolStatus {
                log: "ready".to_string(),
                state: tool_status::State::Ready.into(),
            },
            ToolStatus::Preparing => grpc::ToolStatus {
                log: "preparing".to_string(),
                state: tool_status::State::Preparing.into(),
            },
            ToolStatus::Dropped => grpc::ToolStatus {
                log: "preparing".to_string(),
                state: tool_status::State::Preparing.into(),
            },
        }
    }
}

impl From<grpc::ToolStatus> for ToolStatus {
    fn from(status: grpc::ToolStatus) -> Self {
        match status.state() {
            tool_status::State::Preparing => ToolStatus::Preparing,
            tool_status::State::Ready => ToolStatus::Ready,
            tool_status::State::Dropped => ToolStatus::Dropped,
        }
    }
}

#[async_trait::async_trait]
pub trait Dispatcher: Send + Sync + 'static {
    // TODO: for all types involved in the inner traits, should use mirror type of grpc type
    // because these traits are meant to be impl from lib, not from looking at gencode from
    // protobuf. Same for ToolService etc.
    async fn launch(
        &self,
        uid: &str,
        tool: &ToolMeta,
        files: &HashMap<String, FileEntry>,
    ) -> anyhow::Result<String>;
    async fn get_artifact(&self, handler_id: &str) -> anyhow::Result<Artifact>;
    /// get status of a tool from its handler id.
    async fn get_status(&self, handler_id: &str) -> anyhow::Result<ToolStatus>;
    async fn query_tools(&self, uid: &str) -> anyhow::Result<Vec<ToolHandler>>;
}

// NOTE: the flexibility of this abstraction is that I can have the persistency in dispatcher
// Or there is option that the db persistency is a table decoupled from dispatcher.
// I can have a "user states table" to record it if want to keep dispatcher very thin.
pub struct Dataplayer {
    dispatcher: Arc<dyn Dispatcher>,
    tool_source: Arc<dyn ToolSource>,
}

impl Dataplayer {
    pub fn new(dp: Arc<dyn Dispatcher>, tool_src: Arc<dyn ToolSource>) -> Self {
        Self {
            dispatcher: dp,
            tool_source: tool_src,
        }
    }
}

/// Artifact is the collection of information that user (matchmaker) can use to launch the tool
#[derive(Debug, Clone)]
pub enum Artifact {
    HostedTool { callback: Url },
    EoscInlineTool { callback: Url },
}

fn get_user_from_token<T>(req: &Request<T>) -> Result<String, Status> {
    let auth_header = req
        .metadata()
        .get("authorization")
        .ok_or(Status::unauthenticated("Missing authorization header"))?;

    let auth_str = auth_header
        .to_str()
        .map_err(|_| Status::unauthenticated("Invalid authorization header"))?;

    if !auth_str.starts_with("Bearer ") {
        return Err(Status::unauthenticated("Expected Bearer token"));
    }

    let token = &auth_str[7..]; // strip "Bearer "

    // Decode JWT
    let decoding_key = DecodingKey::from_secret(b"my_secret_key"); // or public key if RS256
    let token_data = decode::<Claims>(token, &decoding_key, &Validation::new(Algorithm::HS256))
        .map_err(|_| Status::unauthenticated("Invalid token"))?;

    Ok(token_data.claims.sub)
}

#[derive(Debug, Deserialize, Serialize)]
struct Claims {
    sub: String,
    name: String,
    role: String,
    exp: usize,
}

pub fn create_token() -> String {
    let claims = Claims {
        sub: "user123".to_string(),
        name: "Alice".to_string(),
        role: "admin".to_string(),
        exp: 1_999_999_999, // some expiration
    };
    encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(b"my_secret_key"),
    )
    .unwrap()
}

#[tonic::async_trait]
impl DataplayerService for Dataplayer {
    type MonitorStatusStream = ReceiverStream<Result<MonitorStatusResponse, Status>>;

    async fn launch_tool(
        &self,
        req: Request<LaunchToolRequest>,
    ) -> Result<Response<LaunchToolResponse>, Status> {
        let user = get_user_from_token(&req).unwrap();
        let req = req.get_ref();
        let id = &req.tool_id.clone(); // FIXME: DONTPANIC
        let slots_mapping = &req.slots_mapping;

        let tool_meta = self.tool_source.get_tool(id).await.unwrap();

        let id = self
            .dispatcher
            .launch(&user, &tool_meta, slots_mapping)
            .await
            .unwrap();

        Ok(Response::new(LaunchToolResponse { handler_id: id }))
    }

    async fn query(
        &self,
        req: Request<QueryUserRequest>,
    ) -> Result<Response<QueryUserResponse>, Status> {
        let user = get_user_from_token(&req).unwrap();
        let tools = self.dispatcher.query_tools(&user).await.unwrap();
        Ok(Response::new(QueryUserResponse { ths: tools }))
    }

    async fn get_artifact(
        &self,
        req: Request<GetArtifactRequest>,
    ) -> Result<Response<GetArtifactResponse>, Status> {
        let req = req.get_ref();
        let handler_id = &req.handler_id;
        // TODO: check the state of the tool is ready.
        let status = self.dispatcher.get_status(handler_id).await.unwrap();
        if !matches!(status, ToolStatus::Ready) {
            return Err(Status::internal("tool not ready"));
        }
        let artifact = self.dispatcher.get_artifact(handler_id).await.unwrap();

        let ep = match artifact {
            Artifact::HostedTool { callback } => {
                let hosted_tool = grpc::HostedTool {
                    callback_url: callback.to_string(),
                };
                EntryPoint::Hosted(hosted_tool)
            }
            Artifact::EoscInlineTool { callback } => {
                let hosted_tool = grpc::EoscInlineTool {
                    callback_url: callback.to_string(),
                };
                EntryPoint::EoscInline(hosted_tool)
            }
        };

        Ok(Response::new(GetArtifactResponse {
            entry_point: Some(ep),
        }))
    }

    async fn get_status(
        &self,
        req: Request<GetStatusRequest>,
    ) -> Result<Response<GetStatusResponse>, Status> {
        let req = req.get_ref();
        let handler_id = &req.id;
        let tool_status = self.dispatcher.get_status(handler_id).await.unwrap();

        Ok(Response::new(GetStatusResponse {
            status: Some(tool_status.into()),
        }))
    }

    async fn monitor_status(
        &self,
        req: Request<MonitorStatusRequest>,
    ) -> Result<Response<Self::MonitorStatusStream>, Status> {
        let (tx, rx) = mpsc::channel(16);

        tokio::spawn(async move {
            tx.send(Ok(MonitorStatusResponse {
                status: Some(ToolStatus::Ready.into()),
            }))
            .await
            .ok();
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn drop(&self, req: Request<DropRequest>) -> Result<Response<DropResponse>, Status> {
        todo!()
    }
}

// FIXME: look at EC2 etc, to have a better list of required fields
#[derive(Debug)]
struct EnvResource {
    num_cpu: u32,
    num_ram: u64,
}

/// Config for how to launch the VRE, these are specifically for e.g. `.binder`.
/// The resource description is independent of this config.
/// The request packager do not (should not??, but if tool-registry also strong typed, maybe I can
/// constructed the type easily here??) know the exact format of the config. The format is
/// encoded in the tool-registry and know b
/// TODO: if the overall architecture and tech stack can not change (ask Enol whether he want to
/// uptake the grpc in more broad scope in dispacher and tool-registry). Otherwise, check if
/// RO-crate can provide such level of schema check.
#[derive(Debug)]
struct Config {
    inner: serde_json::Value,
}

#[derive(Debug)]
pub enum VirtualResearchEnv {
    // tool that opened inline in the page.
    EoscInline {
        id: String,
        version: String,
    },

    // tool that redirect to 3rd-party site with the selected files
    // such tools are very lightweight and do not need to specify resources.
    BrowserNative {
        id: String,
        files: Vec<PathBuf>,
    },

    // tool that need VM resources and have resources attached (e.g. RRP, Galaxy)
    Hosted {
        id: String,
        version: String,
        // TODO: String is too vague, here I expect a describle requirements on configs and
        // required files, that the server side can use to validate.
        requirements: Vec<String>,
    },

    // (planned):
    // Hosted but required resources provided
    // - allow to allocating using EOSC resources.
    // - allow to asking for tools that provide resourecs.
    // I have a felling that this should be a special type of vre, because in the Assembler
    // service, I make it non-stream rpc call, the resource requests need back and forth comm
    // between client and server, therefore better managed with bilateral streams.
    HostedWithoutRes {
        id: String,
        config: Option<Config>,
        files: Vec<PathBuf>,
        res: EnvResource,
    },
}

// impl VirtualResearchEnv {
//     pub fn attach_files(files: &Vec<FileEntry>) {
//         todo!()
//     }
// }

// TODO: have a protobuf defined for the VirtualResearchEnv and mapping conversion here
//
// impl From<proto::VirtualResearchEnv> for VirtualResearchEnv {
//     fn from(value: proto::VirtualResearchEnv) -> Self {
//         match value {
//             =>
//             =>
//             =>
//             =>
//         }
//     }
// }

// server side call this function to assemble a payload that can send to downstream dispacher
// XXX: the return type is a very generic json, I probably want a crate to handle ro-crate
// specificly.
fn assemble_vre_request(vre: &VirtualResearchEnv) -> serde_json::Value {
    match vre {
        VirtualResearchEnv::EoscInline { .. } => todo!(),
        VirtualResearchEnv::BrowserNative { .. } => todo!(),
        VirtualResearchEnv::Hosted { .. } => todo!(),
        VirtualResearchEnv::HostedWithoutRes { .. } => todo!(),
    }
}
