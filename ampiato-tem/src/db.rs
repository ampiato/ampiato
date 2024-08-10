use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    hash::Hash,
    rc::Rc,
};

use crate::replication::{
    from_tuple_data::TableFromTupleData, pgoutput::LogicalReplicationMessage,
    replication::Replication,
};
use crate::{
    core::{
        defs::{Index, Time},
        TableValues,
    },
    Error,
};
use colored::Colorize as _;
use petgraph::matrix_graph::{MatrixGraph, NodeIndex};
use sqlx::PgPool;

use super::value_provider::ValueProvider;

pub type NodeT<Sel> = (&'static str, Sel, Time);
pub type DepGraph<Sel> = MatrixGraph<NodeT<Sel>, (), petgraph::Directed, Option<()>, usize>;

pub struct Db<Sel, T, VP>
where
    Sel: Clone + Eq + Hash,
    T: TableFromTupleData + TableValues<Sel>,
    VP: ValueProvider<Sel>,
{
    value_provider: VP,
    refs: Rc<RefCell<HashMap<NodeT<Sel>, NodeIndex<usize>>>>,
    deps: Rc<RefCell<DepGraph<Sel>>>,

    dep_tracing_stack: Rc<RefCell<Vec<HashSet<Index>>>>,

    replication: Rc<RefCell<Option<Replication>>>,

    subs: HashMap<Index, NodeT<Sel>>,
    // value_cache: HashMap<NodeT<Sel>, f64>,
    verbose: bool,

    phantom: std::marker::PhantomData<T>,
}

// type SubsciptionId = usize;

impl<Sel, T, VP> Db<Sel, T, VP>
where
    Sel: Clone + Eq + Hash,
    T: TableFromTupleData + TableValues<Sel>,
    VP: ValueProvider<Sel>,
{
    pub async fn from_env(replication: bool) -> Result<Self, sqlx::Error> {
        let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
        Self::from_database_url(&database_url, replication).await
    }

    pub async fn from_database_url(
        database_url: &str,
        replication: bool,
    ) -> Result<Self, sqlx::Error> {
        let pool = PgPool::connect(database_url).await?;
        Self::from_pool(&pool, replication).await
    }

    pub async fn from_pool(pool: &PgPool, replication: bool) -> Result<Self, sqlx::Error> {
        let vp = VP::from_pool(pool).await;

        let replication = if replication {
            Some(Replication::from_pool(pool).await?)
        } else {
            None
        };

        Ok(Db {
            value_provider: vp,
            refs: Rc::new(RefCell::new(HashMap::new())),
            deps: Rc::new(RefCell::new(DepGraph::default())),
            dep_tracing_stack: Rc::new(RefCell::new(Vec::new())),
            replication: Rc::new(RefCell::new(replication)),
            subs: HashMap::new(),
            // value_cache: HashMap::new(),
            verbose: false,
            phantom: std::marker::PhantomData,
        })
    }

    pub async fn stop_replication(&self) -> Result<(), Error> {
        let mut replication = self.replication.borrow_mut();
        if let Some(replication) = replication.as_mut() {
            replication.close_and_cleanup().await?;
        }
        Ok(())
    }

    pub async fn sync_changes(&mut self) -> Result<HashSet<Index>, Error> {
        let changes = {
            let mut replication = self.replication.borrow_mut();
            let replication = replication.as_mut();
            if let Some(replication) = replication {
                replication.grab_changes().await?
            } else {
                return Err(Error::ReplicationNotEnabled);
            }
        };

        let mut is_in_transaction = false;
        let mut transaction_messages = Vec::new();
        let mut updated_subscribers = HashSet::new();
        for change in changes.into_iter() {
            match change {
                LogicalReplicationMessage::Begin(_) => {
                    if !transaction_messages.is_empty() {
                        panic!("Begin message found in the middle of transaction.");
                    }
                    is_in_transaction = true;
                }
                LogicalReplicationMessage::Commit(_) => {
                    if !is_in_transaction {
                        panic!("Commit message found outside of transaction.");
                    }
                    is_in_transaction = false;
                    let updated_ids = self.apply_transaction(&transaction_messages)?;
                    updated_subscribers.extend(updated_ids);
                    transaction_messages.clear();
                }
                _ => {
                    if !is_in_transaction {
                        panic!("Change message found outside of transaction.");
                    }
                    transaction_messages.push(change);
                }
            }
        }

        Ok(updated_subscribers)
    }

    fn apply_transaction(
        &mut self,
        messages: &[LogicalReplicationMessage],
    ) -> Result<HashSet<Index>, Error> {
        if self.verbose {
            Self::describe_transacrtion(messages);
        }
        let mut relation_name = "";
        let mut updated_subscribers = HashSet::new();
        for message in messages.iter() {
            match message {
                LogicalReplicationMessage::Relation(r) => {
                    relation_name = &r.relation_name;
                }
                LogicalReplicationMessage::Update(u) => {
                    let table = T::from_tuple_data(relation_name, &u.new_tuple)?;
                    let t = table.time();
                    let sel = table.selector();
                    let values = table.values();
                    for (name, value) in values.iter() {
                        let updated_ids = self.update(name, &sel, &t, **value);
                        updated_subscribers.extend(updated_ids);
                    }
                }
                LogicalReplicationMessage::Insert(i) => {
                    let table = T::from_tuple_data(relation_name, &i.new_tuple)?;
                    let t = table.time();
                    let sel = table.selector();
                    let values = table.values();
                    for (name, value) in values.iter() {
                        let updated_ids = self.update(name, &sel, &t, **value);
                        updated_subscribers.extend(updated_ids);
                    }
                }
                _ => {
                    panic!("Unsupported message type: {:?}", message);
                }
            }
        }
        Ok(updated_subscribers)
    }

    fn describe_transacrtion(messages: &[LogicalReplicationMessage]) {
        println!("{}", "------------------".green().bold());
        let msg = format!("Processing transaction with {} changes", messages.len());
        println!("{}", msg.bold().green().on_blue());
        for change in messages.iter() {
            println!("{:?}\n", change);
        }
        println!("{}", "------------------".green().bold());
    }

    fn get_ref_for_value(&self, name: &'static str, selector: Sel, t: Time) -> Index {
        let key = (name, selector, t);
        {
            let refs = (*self.refs).borrow();
            if let Some(&node_index) = refs.get(&key) {
                return node_index;
            }
        }

        let mut deps = (*self.deps).borrow_mut();
        let index = deps.add_node(key.clone());

        (*self.refs).borrow_mut().insert(key, index);

        index
    }

    pub fn get_value(&self, name: &'static str, selector: Sel, t: Time) -> f64 {
        let v: f64 = self.value_provider.get_value(name, &selector, &t);
        let r#ref = self.get_ref_for_value(name, selector, t);
        self.dep_tracing_add_dep(r#ref);
        v
    }

    pub fn get_value_opt(&self, name: &'static str, selector: Sel, t: Time) -> Option<f64> {
        let v = self.value_provider.get_value_opt(name, &selector, &t);
        let r#ref = self.get_ref_for_value(name, selector, t);
        self.dep_tracing_add_dep(r#ref);
        v
    }

    pub fn register_fn(
        &self,
        name: &'static str,
        selector: Sel,
        t: Time,
        value_fn: impl Fn(&Db<Sel, T, VP>) -> f64,
    ) -> f64 {
        let r#ref = self.get_ref_for_value(name, selector, t);

        // TODO: Load value from the cache first.
        self.dep_tracing_function_call();
        let value = value_fn(self);
        let fn_refs = self.dep_tracing_function_return();
        std::mem::drop(value_fn);

        {
            let mut deps = (*self.deps).borrow_mut();
            for dep in fn_refs {
                deps.update_edge(dep, r#ref, ());
            }
        }

        value
    }

    fn dep_tracing_function_call(&self) {
        let mut stack = self.dep_tracing_stack.borrow_mut();
        stack.push(HashSet::new());
    }

    fn dep_tracing_function_return(&self) -> HashSet<Index> {
        let mut stack = self.dep_tracing_stack.borrow_mut();
        match stack.pop() {
            Some(deps) => deps,
            None => unreachable!(),
        }
    }

    /// Add a dependency from the current node to the given node.
    ///
    /// When the dependency tracing stack is empty, there is no function being evaluated.
    /// It means that the user is retrieving values from the database. There is no need to
    /// track the dependencies in this case.
    fn dep_tracing_add_dep(&self, dep: Index) {
        let mut stack = self.dep_tracing_stack.borrow_mut();
        let last = match stack.last_mut() {
            Some(last) => last,
            None => return,
        };
        last.insert(dep);
    }

    pub fn graph(&self) -> &Rc<RefCell<DepGraph<Sel>>> {
        &self.deps
    }

    /// TODO: The subscription should be a separate object that automatically unsubscribes
    /// when it is dropped.
    pub fn subscribe(&mut self, name: &'static str, selector: &Sel, t: &Time) -> Index {
        let ref_ = self.get_ref_for_value(name, selector.clone(), *t);
        self.subs.insert(ref_, (name, selector.clone(), t.clone()));
        ref_
    }

    pub fn unsubscribe(&mut self, _ref: Index) {
        todo!()
    }

    /// TODO: When updating values in bulk (e.g., in a transaction), we can wait with the
    /// recursive part until the end of the transaction. This way, we can avoid updating
    /// the same values multiple times.
    pub fn update(
        &mut self,
        name: &'static str,
        selector: &Sel,
        t: &Time,
        new_value: f64,
    ) -> HashSet<Index> {
        self.value_provider
            .set_value(name, (*selector).clone(), *t, new_value);

        let mut updated_subscribers = HashSet::new();

        let refs = (*self.refs).borrow();
        let deps = (*self.deps).borrow();

        let r#ref = refs.get(&(name, selector.clone(), t.clone())).unwrap();

        let mut dirty_refs = vec![*r#ref];

        while let Some(ref_) = dirty_refs.pop() {
            if self.subs.contains_key(&ref_) {
                updated_subscribers.insert(ref_);
            }

            for (_, index_to, _) in deps.edges_directed(ref_, petgraph::Direction::Outgoing) {
                dirty_refs.push(index_to);
            }
        }

        updated_subscribers
    }
}
