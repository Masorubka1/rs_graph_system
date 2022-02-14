use crate::{sleep_ms, HashMap};
use crate::{Ans, Xz};

pub fn do_smth_2(tmp: &HashMap<&str, Option<usize>>) -> Xz {
    println!("{}", tmp["name"].unwrap());
    sleep_ms(400);
    Xz::new(5)
}
pub struct Worker {
    pub status: bool,
    num: usize,
}

impl Worker {
    pub fn new(a: usize) -> Worker {
        Worker {
            num: a,
            status: false,
        }
    }
    pub async fn execute(
        &mut self,
        func_id: usize,
        args_id: usize,
        res_id: usize,
        a: &mut Ans,
    ) -> Option<bool> {
        self.status = true;
        a.RES_LIST[res_id] = Some(Box::new(a.FN_LIST[func_id](&a.ARGS_LIST[args_id])));
        Some(true)
    }
}
