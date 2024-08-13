use crate::{
    pb::{
        etcdserverpb::{kv_server, PutRequest, PutResponse, RangeRequest, RangeResponse, ResponseHeader},
        mvccpb::KeyValue,
    },
    Client, ClusterId, LeaseId, MemberId, Revision, Term, Version,
};
use std::{
    collections::BTreeMap,
    net::{SocketAddr, TcpListener},
    num::{NonZeroU16, NonZeroU64},
    sync::{Arc, RwLock},
};
use tonic::{transport::Server, Request, Response, Status};

type GrpcResult<T> = Result<Response<T>, Status>;

pub struct FakeServer {
    inner: Arc<FakeServerInner>,
    host_addr: SocketAddr,
}

impl FakeServer {
    pub fn builder() -> FakeServerBuilder {
        FakeServerBuilder::default()
    }

    pub fn lazy_client(&self) -> Client {
        Client::new(&format!("http://{}", self.host_addr)).unwrap()
    }

    pub async fn run(&self) {
        let server = Server::builder().add_service(kv_server::KvServer::from_arc(self.inner.clone()));
        server.serve(self.host_addr).await.unwrap();
    }
}

pub struct FakeServerBuilder {
    cluster_id: ClusterId,
    member_id: MemberId,
    raft_term: Term,
    revision: Option<Revision>,
    entries: Vec<(Vec<u8>, Entry)>,
    insecure_port: Option<NonZeroU16>,
}

impl FakeServerBuilder {
    pub fn build(self) -> Result<FakeServer, String> {
        let revision = if let Some(revision) = self.revision {
            revision
        } else {
            self.entries
                .iter()
                .map(|(_, e)| e.mod_revision)
                .max()
                .unwrap_or(Revision::new(1).unwrap())
        };
        let insecure_port = self.insecure_port.unwrap_or_else(|| {
            // ask the OS to give us an unused TCP port -- expect that nobody grabs it from now to we use it later
            let sock = TcpListener::bind("127.0.0.1:0").expect("failed to bind to socket");
            NonZeroU16::new(sock.local_addr().unwrap().port()).unwrap()
        });

        let contents = FakeServerContents {
            meta: ServerMetaState {
                cluster_id: self.cluster_id,
                member_id: self.member_id,
                revision,
                raft_term: self.raft_term,
            },
            entries: self.entries.into_iter().collect(),
        };
        Ok(FakeServer {
            inner: Arc::new(FakeServerInner {
                contents: RwLock::new(contents),
            }),
            host_addr: format!("127.0.0.1:{insecure_port}").parse().unwrap(),
        })
    }
}

impl Default for FakeServerBuilder {
    fn default() -> Self {
        Self {
            cluster_id: ClusterId(NonZeroU64::new(1).unwrap()),
            member_id: MemberId(NonZeroU64::new(1).unwrap()),
            raft_term: Term(NonZeroU64::new(1).unwrap()),
            revision: None,
            entries: Vec::new(),
            insecure_port: None,
        }
    }
}

struct FakeServerInner {
    contents: RwLock<FakeServerContents>,
}

struct FakeServerContents {
    meta: ServerMetaState,
    entries: BTreeMap<Vec<u8>, Entry>,
}

struct ServerMetaState {
    cluster_id: ClusterId,
    member_id: MemberId,
    revision: Revision,
    raft_term: Term,
}

impl ServerMetaState {
    fn response_header(&self) -> ResponseHeader {
        ResponseHeader {
            cluster_id: self.cluster_id.0.get(),
            member_id: self.member_id.0.get(),
            revision: self.revision.0.get(),
            raft_term: self.raft_term.0.get(),
        }
    }
}

struct Entry {
    value: Vec<u8>,
    version: Version,
    create_revision: Revision,
    mod_revision: Revision,
    lease: Option<LeaseId>,
}

impl Entry {
    fn to_key_value(&self, key: Vec<u8>) -> KeyValue {
        KeyValue {
            key,
            create_revision: self.create_revision.0.get(),
            mod_revision: self.mod_revision.0.get(),
            version: self.version.0 as i64,
            value: self.value.clone(),
            lease: self.lease.map_or(0, |x| x.0.get()),
        }
    }
}

#[tonic::async_trait]
impl kv_server::Kv for FakeServerInner {
    async fn range(self: Arc<Self>, request: Request<RangeRequest>) -> GrpcResult<RangeResponse> {
        let req = request.into_inner();
        let contents = self.contents.read().unwrap();

        let resp = if let Some(entry) = contents.entries.get(&req.key) {
            RangeResponse {
                header: Some(contents.meta.response_header()),
                kvs: vec![entry.to_key_value(req.key)],
                more: false,
                count: 1,
            }
        } else {
            RangeResponse {
                header: Some(contents.meta.response_header()),
                kvs: Vec::new(),
                more: false,
                count: 0,
            }
        };
        Ok(Response::new(resp))
    }

    async fn put(self: Arc<Self>, request: Request<PutRequest>) -> GrpcResult<PutResponse> {
        let req = request.into_inner();
        let mut contents = self.contents.write().unwrap();

        contents.meta.revision.next();
        let revision = contents.meta.revision;

        let prev_kv = if let Some(existing) = contents.entries.get_mut(&req.key) {
            let saved = if req.prev_kv {
                Some(existing.to_key_value(req.key))
            } else {
                None
            };

            existing.mod_revision = revision;
            existing.version.next();

            saved
        } else {
            contents.entries.insert(
                req.key,
                Entry {
                    value: req.value,
                    version: Version::default(),
                    create_revision: revision,
                    mod_revision: revision,
                    lease: None,
                },
            );

            None
        };

        Ok(Response::new(PutResponse {
            header: Some(contents.meta.response_header()),
            prev_kv,
        }))
    }
}
