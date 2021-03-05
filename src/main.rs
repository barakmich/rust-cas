#[macro_use]
extern crate rocket;

use std::path::PathBuf;

use lazy_static::lazy_static;

use sha2::Digest;

use regex::Regex;

use rocket::data::ToByteUnit;
use rocket::response::Result;
use rocket::tokio::fs::File;
use rocket::tokio::io::AsyncReadExt;
use rocket::tokio::io::AsyncWriteExt;
use rocket::{http::Status, routes, Response};

use tempfile::NamedTempFile;

lazy_static! {
    static ref RE: Regex = Regex::new("[0-9a-fA-F]{64}").unwrap();
}

// TODO
// - Use {s,d}trace to ensure that temp files are created and cleaned up correctly
// - Catch EXDEV invalid cross-link device on .persist() and copy to new location to work around
//   filesystem boundaries
// - Better Error handling
// - Remove rocket friendly default
//   - CLI output
//   - Override rocket default error catcher

#[get("/<sha>")]
async fn get_cas<'a>(sha: String) -> Result<'a> {
    if !RE.is_match(sha.as_str()) {
        return Err(Status::BadRequest);
    }

    let p: PathBuf = [".", "data", sha.as_str()].iter().collect();
    match File::open(p).await {
        Ok(file) => Ok(Response::build().sized_body(None, file).finalize()),
        Err(_) => Err(Status::InternalServerError),
    }
}

#[put("/<sha>", data = "<data>")]
async fn put_cas<'a>(sha: String, data: rocket::data::Data) -> Result<'a> {
    if !RE.is_match(sha.as_str()) {
        return Err(Status::BadRequest);
    }

    let mut stream = data.open(512.mebibytes());
    let mut bbuf = bytes::BytesMut::with_capacity(4096);
    let mut sum = sha2::Sha256::new();

    let tempfile = NamedTempFile::new().map_err(|_| Status::InternalServerError)?;
    let mut file = File::from_std(tempfile.reopen().map_err(|_| Status::InternalServerError)?);

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

    let temppath = tempfile.into_temp_path();

    if sha.to_ascii_lowercase() != hex::encode(sum.finalize()) {
        return Err(Status::BadRequest);
    }

    let p: PathBuf = [".", "data", sha.as_str()].iter().collect();
    temppath
        .persist_noclobber(p)
        .map_err(|_| Status::InternalServerError)?;
    Ok(Response::build().status(Status::Created).finalize())
}

#[launch]
fn rocket() -> rocket::Rocket {
    rocket::ignite().mount("/cas", routes![put_cas, get_cas])
}
