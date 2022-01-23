use std::thread::sleep_ms;
use std::sync::Arc;
use std::collections::HashMap;
use std::ptr;
use rs_graph_system::ThreadPool;
use petgraph::Graph;
use petgraph::prelude::Bfs;
use petgraph::adj::NodeIndex;
use petgraph::adj::IndexType;
use petgraph::algo::{toposort, DfsSpace};

pub struct xz {
    xz: i32
}

type Unar = fn(i32, i32) -> i32;

impl xz {
    fn new(num: i32) -> xz {
        xz {xz: num}
    }
}

pub struct InfoNode<T> {
    func: &'static str,
    args: HashMap<Box<str>, u32>,
    res: Box<T>
}

impl InfoNode<xz> {
    fn new(name: String, Args: HashMap<Box<str>, u32>) -> InfoNode<xz>{
        InfoNode {func: Box::leak(name.into_boxed_str()), args: Args, res: Box::new(xz::new(5))}
    }
    fn execute(self) {
        self.res = Box::new(self.func(self.args));
    }
}

fn test_build_graph(deps: &mut Graph<&str, &str>) -> HashMap<usize, NodeIndex> {
    let arr = vec!["petgraph", "fixedbitset", "quickcheck", "rand", "libc"];
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
    deps.extend_with_edges(&[
        (pg, fb, "1"), (pg, qc, "2"),
        (qc, rand, "3"), (rand, libc, "4"), (qc, libc, "5"), (fb, libc, "6")
    ]);
    list_nodes
}

fn do_smth(a: &str) -> ()
{
    println!("{}", a);
    sleep_ms(400)
}

fn main() {
    let mut deps = Graph::<&str, &str>::new();
    let pool = ThreadPool::new(4);
    let list_nodes;
    {
        list_nodes = test_build_graph(&mut deps);
    }
    let mut space = DfsSpace::new(&deps);  
    let res = toposort(&deps, Some(&mut space)).unwrap();
    println!("{:?}", res);
    //let res = timesort(&deps, &NodeIndex::new(0));
    /*for i in res {
        println!("{:?}", deps[i.index()]);
    }*/
    //print!("{:?}", res);
    let vect_fn = vec![do_smth];
    for i in res {
        let id = list_nodes[&i.index()];
        let tmp = NodeIndex::new(id.try_into().unwrap());
        let weight = deps.node_weight(tmp).unwrap().to_string();
        let funct = Arc::new(vect_fn[0]);
        pool.execute(move || {
            funct(&weight);
        });
    }
}