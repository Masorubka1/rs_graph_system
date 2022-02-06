use petgraph::adj::IndexType;
use petgraph::adj::NodeIndex;
use petgraph::Graph;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::thread::sleep_ms;
mod producer;
use crate::producer::Prod;
mod consumer;
use crate::consumer::Cons;

#[derive(Copy, Clone)]
pub struct Xz {
    Xz: usize,
}

impl Xz {
    fn new(num: usize) -> Xz {
        Xz { Xz: num }
    }
}

#[derive(Clone)]
pub struct InfoNode<'a, T, V> {
    func: fn(&HashMap<&'a str, Option<T>>) -> V,
    args: HashMap<&'a str, Option<T>>,
    res: Option<Box<V>>,
}

impl<'a> InfoNode<'a, usize, Xz> {
    fn new(
        name: fn(&HashMap<&'a str, Option<usize>>) -> Xz,
        Args: HashMap<&'a str, Option<usize>>,
    ) -> InfoNode<'a, usize, Xz> {
        InfoNode {
            func: name,
            args: Args,
            res: Some(Box::new(Xz::new(0))),
        }
    }
    fn execute(helper: InfoNode<'a, usize, Option<Xz>>) -> Box<Xz> {
        let tmp = ((helper.func)(&helper.args)).unwrap();
        Box::new(tmp)
    }
    fn execute_self(&mut self) {
        self.res = Some(Box::new((self.func)(&self.args)));
    }
}

impl<'a> Default for InfoNode<'a, usize, Xz> {
    fn default() -> Self {
        InfoNode {
            func: do_smth_2,
            args: HashMap::<&'a str, Option<usize>>::new(),
            res: Some(Box::<Xz>::new(Xz::new(0))),
        }
    }
}

fn test_build_graph(deps: &mut Graph<InfoNode<usize, Xz>, &str>) -> HashMap<usize, NodeIndex> {
    let mut first_h = HashMap::new();
    first_h.insert("name", Some(0));
    let mut second_h = HashMap::new();
    second_h.insert("name", Some(1));
    let mut third_h = HashMap::new();
    third_h.insert("name", Some(2));
    let mut fourth_h = HashMap::new();
    fourth_h.insert("name", Some(3));
    let mut thith_h = HashMap::new();
    thith_h.insert("name", Some(4));
    let first = InfoNode::new(do_smth_2, first_h);
    let second = InfoNode::new(do_smth_2, second_h);
    let third = InfoNode::new(do_smth_2, third_h);
    let fourth = InfoNode::new(do_smth_2, fourth_h);
    let thith = InfoNode::new(do_smth_2, thith_h);

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

fn do_smth_2(tmp: &HashMap<&str, Option<usize>>) -> Xz {
    println!("{}", tmp["name"].unwrap());
    sleep_ms(400);
    Xz::new(5)
}

fn timesort(deps: &Graph<InfoNode<usize, Xz>, &str>, ind: NodeIndex) -> Vec<isize> {
    let mut hash_nodes = HashSet::<usize>::new();
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
        hash_nodes.insert(node);
        for i in deps.neighbors_directed(tmp_node_index, petgraph::EdgeDirection::Outgoing) {
            if ans[i.index()] == -1 {
                queue_nodes.push_back(i.index());
                hash_nodes.insert(i.index());
                ans[i.index()] = -2;
            } else {
                hash_nodes.remove(&i.index());
            }
        }
        let mut f = 0;
        for i in deps.neighbors_directed(tmp_node_index, petgraph::EdgeDirection::Incoming) {
            if hash_nodes.contains(&i.index()) {
                f = 1;
                break;
            }
        }
        if f == 1 {
            queue_nodes.push_back(node);
        } else {
            ans[node] = cnt;
            cnt += 1;
            hash_nodes.remove(&node);
        }
    }
    ans
}

fn main() {
    let mut deps = Graph::<InfoNode<usize, Xz>, &str>::new();
    let _list_nodes;
    {
        _list_nodes = test_build_graph(&mut deps);
    }
    let sorted_nodes = timesort(&deps, NodeIndex::new(0));
    println!("{:?}", sorted_nodes);
    let mut Produc = Prod::new();
    let mut Consum = Cons::new("quickstart-events");
    let mut q = VecDeque::<isize>::new();
    for i in sorted_nodes {
        Produc.writ("quickstart-events", &format!("{} {}", &"Start", i));
        q.push_back(i);
    }
    while q.len() != 0 {
        for ms in Consum.cons.poll().unwrap().iter() {
            for m in ms.messages() {
                let value = match std::str::from_utf8(m.value) {
                    Ok(v) => v,
                    Err(e) => panic!("Invalid UTF-8 sequence: {}", e),
                };
                let mut var = value.split(" ");
                let status = var.next().unwrap();
                let node_id_str = var.next().unwrap();
                let tmp = node_id_str.parse::<usize>();
                if tmp.is_err() {
                    continue;
                }
                let node_id = tmp.unwrap().index();
                match status {
                    "Start" => {
                        let mut info_node = deps
                            .node_weight_mut(NodeIndex::new(node_id))
                            .unwrap()
                            .to_owned();
                        let mut stop = false;
                        for (_i, xz) in &info_node.args {
                            if xz.is_none() {
                                stop = true;
                                break;
                            }
                            let cur_node_arg = xz.unwrap();
                            stop = match deps.node_weight(NodeIndex::new(cur_node_arg)).unwrap().res {
                                None => true,
                                _ => false,
                            };
                            if stop == true && node_id != 0 {
                                break;
                            }
                        }
                        match stop {
                            true => {
                                Produc.writ("quickstart-events", &format!("Not {}", node_id));
                            }
                            false => {
                                info_node.execute_self();
                                Produc.writ("quickstart-events", &format!("Ok {}", node_id));
                                q.pop_front();
                            }
                        }
                    }
                    "Not" => {
                        Produc.writ("quickstart-events", &format!("Start {}", node_id));
                    }
                    _ => {
                        println!("finished, {:}", node_id);
                    }
                }
            }
            Consum.cons.consume_messageset(ms);
        }
        Consum.cons.commit_consumed().unwrap();
    }
    /*for i in sorted_nodes {
        let id = list_nodes[&i.try_into().unwrap()];
        let node_id = NodeIndex::new(id.try_into().unwrap());
        {
            let mut info_node = deps.node_weight_mut(node_id).unwrap().to_owned();
            pool.execute(move || {
                info_node.execute_self();
            });
            //println!("{:}", deps.node_weight_mut(node_id).unwrap().res.Xz)
        }
    }*/
}
