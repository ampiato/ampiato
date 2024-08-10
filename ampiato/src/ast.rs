// use crate::core::defs::{join_refs, Bool, F64};


// pub fn lt(a: F64, b: F64) -> Bool {
//     let refs = join_refs(&a, &b);
//     Bool::new_with_refs(a.get_val() < b.get_val(), refs)
// }

// pub fn if_(cond: Bool, a: F64, b: F64) -> F64 {
//     if *cond.get_val() {
//         F64::new_with_refs(*a.get_val(), join_refs(&cond, &a))
//     } else {
//         F64::new_with_refs(*b.get_val(), join_refs(&cond, &b))
//     }
// }


// pub fn min(a: F64, b: F64) -> F64 {
//     let refs = join_refs(&a, &b);
//     let v = if_(lt(a.clone(), b.clone()), a, b);
//     F64::new_with_refs(*v.get_val(), refs)
// }
