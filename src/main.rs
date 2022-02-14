use kafka::consumer::Message;
use petgraph::adj::IndexType;
use petgraph::adj::NodeIndex;
use petgraph::Graph;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::num::ParseIntError;
use std::thread::sleep_ms;
mod producer;
use crate::producer::Prod;
mod consumer;
use crate::consumer::Cons;
mod worker;
use crate::worker::{do_smth_2, Worker};
use futures::executor::block_on;

pub struct Ans {
    FN_LIST: Vec<fn(&HashMap<&'static str, Option<usize>>) -> Xz>,
    ARGS_LIST: Vec<HashMap<&'static str, Option<usize>>>,
    RES_LIST: Vec<Option<Box<Xz>>>,
}

impl Ans {
    pub fn new() -> Ans {
        Ans {
            FN_LIST: Vec::new(),
            ARGS_LIST: Vec::new(),
            RES_LIST: Vec::new(),
        }
    }
}

#[derive(Copy, Clone)]
pub struct Xz {
    xz: usize,
}

impl Xz {
    fn new(num: usize) -> Xz {
        Xz { xz: num }
    }
}

#[derive(Clone)]
pub struct InfoNode<'a, T, V> {
    id: usize,
    func: fn(&HashMap<&'a str, Option<T>>) -> V,
    args: HashMap<&'a str, Option<T>>,
    res: Option<Box<V>>,
}

impl<'a> InfoNode<'a, usize, Xz> {
    fn new(
        name: fn(&HashMap<&'a str, Option<usize>>) -> Xz,
        Args: HashMap<&'a str, Option<usize>>,
        num: usize,
    ) -> InfoNode<'a, usize, Xz> {
        InfoNode {
            id: num,
            func: name,
            args: Args,
            res: None,
        }
    }
    fn execute_self(&mut self) {
        self.res = Some(Box::new((self.func)(&self.args)));
    }
}

impl<'a> Default for InfoNode<'a, usize, Xz> {
    fn default() -> Self {
        InfoNode {
            id: 0,
            func: do_smth_2,
            args: HashMap::<&'a str, Option<usize>>::new(),
            res: Some(Box::<Xz>::new(Xz::new(0))),
        }
    }
}

fn test_build_graph(
    deps: &mut Graph<InfoNode<usize, Xz>, &str>,
    a: &mut Ans,
) -> HashMap<usize, NodeIndex> {
    let mut first_h = HashMap::new();
    first_h.insert("name", Some(0));
    let mut second_h = HashMap::new();
    second_h.insert("name", Some(0));
    let mut third_h = HashMap::new();
    third_h.insert("name", Some(0));
    let mut fourth_h = HashMap::new();
    fourth_h.insert("name", Some(1));
    let mut thith_h = HashMap::new();
    thith_h.insert("name", Some(2));
    a.ARGS_LIST.push(first_h.clone());
    a.ARGS_LIST.push(second_h.clone());
    a.ARGS_LIST.push(third_h.clone());
    a.ARGS_LIST.push(fourth_h.clone());
    a.ARGS_LIST.push(thith_h.clone());
    for _ in 0..5 {
        a.FN_LIST.push(do_smth_2);
        a.RES_LIST.push(None);
    }
    let first = InfoNode::new(do_smth_2, first_h, 0);
    let second = InfoNode::new(do_smth_2, second_h, 1);
    let third = InfoNode::new(do_smth_2, third_h, 2);
    let fourth = InfoNode::new(do_smth_2, fourth_h, 3);
    let thith = InfoNode::new(do_smth_2, thith_h, 4);

    //let arr = vec!["petgraph", "fixedbitset", "quickcheck", "rand", "libc"];
    let arr = vec![first, second, third, fourth, thith];
    let mut list_nodes = HashMap::<usize, NodeIndex>::new();
    let mut tmp_cnt = 0;
    for i in arr {
        list_nodes.insert(tmp_cnt, deps.add_node(i).index().try_into().unwrap());
        tmp_cnt += 1;
    }

    let pg = list_nodes[&0];
    let fb = list_nodes[&1];
    let qc = list_nodes[&2];
    let rand = list_nodes[&3];
    let libc = list_nodes[&4];

    deps.extend_with_edges(&[(pg, fb), (pg, qc), (qc, rand), (rand, libc), (qc, libc)]);
    list_nodes
}

fn timesort(deps: &Graph<InfoNode<usize, Xz>, &str>, ind: NodeIndex) -> Vec<isize> {
    let mut queue_nodes = VecDeque::<usize>::new();
    let mut ans = Vec::<isize>::new();
    for _ in 0..deps.node_count() {
        ans.push(-1);
    }
    let mut cnt = 0;
    queue_nodes.push_back(ind.index().try_into().unwrap());
    while queue_nodes.len() != 0 {
        let node = queue_nodes.pop_front().unwrap();
        let tmp_node_index = NodeIndex::new(node);
        for i in deps.neighbors_directed(tmp_node_index, petgraph::EdgeDirection::Outgoing) {
            if ans[i.index()] == -1 {
                queue_nodes.push_back(i.index());
                ans[i.index()] = -2;
            }
        }
        let mut f = 0;
        for i in deps.neighbors_directed(tmp_node_index, petgraph::EdgeDirection::Incoming) {
            if ans[i.index()] < 0 {
                f = 1;
                break;
            }
        }
        if f == 1 {
            queue_nodes.push_back(node);
        } else {
            ans[node] = cnt;
            cnt += 1;
        }
    }
    ans
}

fn get_message<'a>(m: &'a Message) -> (&'a str, Result<usize, ParseIntError>) {
    let value = match std::str::from_utf8(m.value) {
        Ok(v) => v,
        Err(e) => panic!("Invalid UTF-8 sequence: {}", e),
    };
    let mut var = value.split(" ");
    let status = var.next().unwrap();
    let node_id_str = var.next().unwrap();
    let tmp = node_id_str.parse::<usize>();
    (status, tmp)
}

fn main() {
    let mut deps = Graph::<InfoNode<usize, Xz>, &str>::new();
    let mut a = Ans::new();
    let mut workers = Vec::new();
    let _list_nodes;
    {
        _list_nodes = test_build_graph(&mut deps, &mut a);
        for i in 0..4 {
            workers.push(Worker::new(i));
        }
    }
    let sorted_nodes = timesort(&deps, NodeIndex::new(0));
    println!("{:?}", sorted_nodes);
    let mut produc = Prod::new();
    let mut consum = Cons::new("quickstart-events");
    let mut q = VecDeque::<isize>::new();
    for i in sorted_nodes {
        produc.writ("quickstart-events", &format!("{} {}", &"Start", i));
        q.push_back(i);
    }
    while q.len() != 0 {
        for ms in consum.cons.poll().unwrap().iter() {
            for m in ms.messages() {
                let res = get_message(m);
                let status = res.0;
                let cur_num = res.1;
                if cur_num.is_err() {
                    continue;
                }
                let node_id = cur_num.unwrap().index();
                match status {
                    "Start" => {
                        let info_node = deps.node_weight(NodeIndex::new(node_id)).unwrap();
                        let mut stop = false;
                        for (_i, xz) in &info_node.args {
                            let is_exist_res_xz =
                                &deps.node_weight(NodeIndex::new(xz.unwrap())).unwrap().res;
                            if is_exist_res_xz.is_none() && node_id != 0 {
                                stop = true;
                                break;
                            }
                        }
                        match stop {
                            true => {
                                produc.writ("quickstart-events", &format!("Not {}", node_id));
                            }
                            false => {
                                let node_info_local =
                                    deps.node_weight_mut(NodeIndex::new(node_id)).unwrap();
                                //node_info_local.execute_self();
                                let mut f = true;
                                while f {
                                    for i in 0..workers.len() {
                                        if !workers[i].status {
                                            f = false;
                                            block_on(workers[i].execute(
                                                node_info_local.id,
                                                node_info_local.id,
                                                node_info_local.id,
                                                &mut a,
                                            ));
                                            node_info_local.res =
                                                a.RES_LIST[node_info_local.id].clone();
                                            workers[i].status = false;
                                            break;
                                        }
                                    }
                                }
                                let tmp = &deps
                                    .node_weight(NodeIndex::new(node_id))
                                    .unwrap()
                                    .to_owned();
                                println!(
                                    "finished, {}, info_node.res = {}",
                                    node_id,
                                    tmp.res.as_ref().unwrap().xz
                                );
                                q.pop_front();
                            }
                        }
                    }
                    "Not" => {
                        produc.writ("quickstart-events", &format!("Start {}", node_id));
                    }
                    _ => {
                        println!("WTF?, {}", node_id);
                    }
                }
            }
            consum.cons.consume_messageset(ms);
        }
        consum.cons.commit_consumed().unwrap();
    }
}
