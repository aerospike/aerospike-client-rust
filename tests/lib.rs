#![allow(dead_code)]
#![allow(unused_imports)]
// #![allow(non_camel_case_types)]

#[macro_use]
extern crate aerospike;
extern crate env_logger;
extern crate rand;
#[macro_use]
extern crate lazy_static;

pub mod common1 {
    use std::u16;
    use std::env;
    use std::sync::Arc;

    use rand;
    use rand::Rng;

    use aerospike::*;

    pub fn rand_str(sz: usize) -> String {
        rand::thread_rng()
            .gen_ascii_chars()
            .take(sz)
            .collect()
    }

    lazy_static! {
	    pub static ref AEROSPIKE_HOST: String = match env::var("AEROSPIKE_HOST") {
	        Ok(s) => s,
	        Err(e) => "127.0.0.1".to_string(),
	    };

	    pub static ref AEROSPIKE_PORT: u16 = match env::var("AEROSPIKE_PORT") {
	        Ok(s) => s.parse().unwrap(),
	        Err(e) => 3000,
	    };
	    pub static ref AEROSPIKE_NAMESPACE: String = match env::var("AEROSPIKE_NAMESPACE") {
	        Ok(s) => s,
	        Err(e) => "test".to_string(),
	    };
	    pub static ref GLOBAL_CLIENT_POLICY: ClientPolicy = {
	    	let mut cp = ClientPolicy::default();
	    	match env::var("AEROSPIKE_USER") {
	    		Ok(user) => {
	    			let pass =  match env::var("AEROSPIKE_PASSWORD") {
	    				Ok(pass) => pass,
	    				Err(_) => "".to_string(),
	    			};

	    			cp.set_user_password(Some((user, pass)));
	    		}
	    		Err(_) => (),
	    	}

	    	cp
	    };
	    pub static ref GLOBAL_CLIENT: Arc<Client> = {
	    	Arc::new(Client::new(&GLOBAL_CLIENT_POLICY, &vec![Host::new(&AEROSPIKE_HOST, *AEROSPIKE_PORT)]).unwrap())
	    };
	}
}

#[macro_use]
mod src;
