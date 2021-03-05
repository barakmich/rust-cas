#[macro_use] extern crate rocket;
use std::path::PathBuf;
use sha2::Digest;

use rocket::{Response, http::Status, routes, tokio};
use rocket::response::Result;
use rocket::data::ToByteUnit;
use rocket::tokio::io::AsyncReadExt;
use rocket::tokio::io::AsyncWriteExt;

// TODO
// Path can escape?
// SHA filename
// Assert SHA equal
// Better Error handling

#[get("/<sha>")]
async fn get_cas<'a>(sha: String) -> Result<'a> {
    let p: PathBuf = [".", "data", sha.as_str()].iter().collect();
    match tokio::fs::File::open(p).await {
        Ok(file) => Ok(Response::build().sized_body(None, file).finalize()),
        Err(_) => Err(Status::InternalServerError),
    }
}

#[put("/<sha>", data = "<data>")]
async fn put_cas<'a>(sha: String, data: rocket::data::Data) -> Result<'a> {
    let p: PathBuf = [".", "data", sha.as_str()].iter().collect();
    let mut stream = data.open(512.mebibytes());
    let mut bbuf = bytes::BytesMut::with_capacity(4096);
    let mut sum = sha2::Sha256::new();
    let mut file = tokio::fs::File::create(p).await.map_err(|err| {
            println!("open {}", err);
            Status::InternalServerError
        })?; 
    
    loop { 
        let cnt = stream.read_buf(&mut bbuf).await.map_err(|err| {
            println!("read {}", err);
            Status::InternalServerError
        })?;
        if cnt == 0 {
            break;
        }
        sum.update(&bbuf);
        file.write_buf(&mut bbuf).await.map_err(|err| {
            println!("write {}", err);
            Status::InternalServerError
        })?;
        bbuf.clear();
    }

    println!("{:x}", sum.finalize());

    Ok(Response::build().status(Status::Created).finalize())
}

#[launch]
fn rocket() -> rocket::Rocket {
    rocket::ignite().mount("/cas", routes![put_cas, get_cas])
}
