use std::net::{ToSocketAddrs, TcpStream};
use std::io::{Result, BufWriter, Write};
use std::fmt::Debug;

use log::{info, error};

pub struct Graphite<A: ToSocketAddrs> {
  addr: A,
  w: BufWriter<TcpStream>,
}

pub type LocalGraphite = Graphite<(&'static str, u16)>;

impl LocalGraphite {
  pub fn new_localhost() -> Result<Self> {
    Self::new(("127.0.0.1", 2003))
  }
}

impl<A: ToSocketAddrs + Debug + Copy> Graphite<A> {
  pub fn new(addr: A) -> Result<Self> {
    let tcp = TcpStream::connect(addr)?;
    let w = BufWriter::new(tcp);
    Ok(Self { addr, w })
  }

  pub fn send_stats<S: AsRef<str>>(&mut self, data: &[S]) {
    info!("sending {} metrics to Graphite {:?}", data.len(), self.addr);
    if let Err(_) = self.send_stats_once(data) {
      // need a reconnect?
      if let Err(e) = self.reconnect() {
        error!("error reconnecting: {:?}", e);
      } else {
        if let Err(e) = self.send_stats_once(data) {
          error!("error: {:?}", e);
        }
      }
    }
  }

  fn reconnect(&mut self) -> Result<()> {
    let tcp = TcpStream::connect(self.addr)?;
    let w = BufWriter::new(tcp);
    self.w = w;
    Ok(())
  }

  fn send_stats_once<S: AsRef<str>>(&mut self, data: &[S]) -> Result<()> {
    for line in data {
      self.w.write_all(line.as_ref().as_bytes())?;
      self.w.write_all(b"\n")?;
    }
    self.w.flush()?;

    Ok(())
  }
}
