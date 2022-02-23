use crate::{serde_json, Cons, Prod, Xz};
use crate::{sleep_ms, HashMap};
use futures::executor::block_on;
use kafka::consumer::Message;

pub fn do_smth_2(tmp: &HashMap<&str, Option<usize>>) -> Xz {
    println!("{}", tmp["name"].unwrap());
    sleep_ms(400);
    Xz::new(5)
}
pub struct Worker {
    status: bool,
    num: usize,
    prod: Prod,
    cons: Cons,
    topic: String,
}

fn get_message<'b>(m: &'b Message) -> (&'b str, usize, usize, &'b str) {
    let value = match std::str::from_utf8(m.value) {
        Ok(v) => v,
        Err(e) => panic!("Invalid UTF-8 sequence: {}", e),
    };
    let mut var = value.split(" ");
    let status = var.next().unwrap();
    let node_id_str = var.next().unwrap();
    let id_node = node_id_str.parse::<usize>().unwrap();
    let name_fn_str = var.next().unwrap();
    let id_fn = name_fn_str.parse::<usize>().unwrap();
    let serialized_args = var.next().unwrap();
    (status, id_node, id_fn, serialized_args)
}

impl<'b> Worker {
    pub fn new(a: usize) -> Worker {
        let prd = Prod::new();
        let cns = Cons::new(&a.to_string());
        Worker {
            num: a,
            status: false,
            prod: prd,
            cons: cns,
            topic: "quickstart-events".to_string(),
        }
    }
    pub async fn execute(
        func: fn(&HashMap<&'b str, Option<usize>>) -> Xz,
        args: HashMap<&'b str, Option<usize>>,
    ) -> Option<Xz> {
        Some(func(&args))
    }
    pub fn run(&mut self) {
        let consum = &mut self.cons.cons;
        let produc = &mut self.prod;
        let vec_func = [do_smth_2, do_smth_2];
        for ms in consum.poll().unwrap().iter() {
            for m in ms.messages() {
                let res = get_message(m);
                let status = res.0;
                let id_node = res.1;
                let id_fn = res.2;
                let serialized_args = res.3;
                match status {
                    "Start" => {
                        let deser: HashMap<&str, Option<usize>> =
                            serde_json::from_str(serialized_args).unwrap();
                        self.status = true;
                        let ans = block_on(Worker::execute(vec_func[id_fn], deser)).unwrap();
                        produc.writ(
                            &self.topic,
                            &format!(
                                "{} {} {} {}",
                                "Finish",
                                id_node,
                                self.num,
                                &ans.xz.to_string()
                            ),
                        );
                    }
                    _ => {}
                }
            }
            let _t = consum.consume_messageset(ms);
            if _t.is_err() {
                panic!("Oh no")
            }
        }
        let _t = consum.commit_consumed();
        if _t.is_err() {
            panic!("Oh no")
        }
        _t.unwrap();
    }
}
