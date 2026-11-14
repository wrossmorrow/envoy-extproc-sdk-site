pub mod controller {
    pub mod v1 {
        include!(concat!(env!("OUT_DIR"), "/controller.v1.rs"));
    }
}

pub mod processor;
