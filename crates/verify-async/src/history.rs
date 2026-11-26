//! Bounded search for a sequential explanation of a complete operation history.

use std::{collections::HashSet, hash::Hash};

pub struct Operation<I, O> {
    pub invoked: u64,
    pub finished: u64,
    pub input: I,
    pub output: O,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Verdict {
    Linearizable(Vec<usize>),
    NonLinearizable,
    BudgetExhausted,
}

/// Enumerate real-time-respecting serializations of up to 64 operations. A
/// transition returns every allowed successor for the observed output. An
/// ambiguous operation can include an unchanged state as well as an effect.
/// Indexes in a successful order refer to the original input slice.
pub fn linearize<I, O, S: Clone + Eq + Hash>(
    history: &[Operation<I, O>],
    initial: S,
    max_states: usize,
    transition: impl FnMut(&S, &I, &O) -> Vec<S>,
) -> Result<Verdict, &'static str> {
    if history.len() > u64::BITS as usize {
        return Err("history exceeds 64 operations");
    }
    if history.iter().any(|op| op.invoked >= op.finished) {
        return Err("operation must finish after invocation");
    }
    let predecessors = history
        .iter()
        .map(|op| {
            history
                .iter()
                .enumerate()
                .filter(|(_, other)| other.finished < op.invoked)
                .fold(0, |mask, (index, _)| mask | (1 << index))
        })
        .collect();
    let mut search = Search {
        history,
        predecessors,
        explored: HashSet::new(),
        order: Vec::with_capacity(history.len()),
        max_states,
        transition,
    };
    Ok(match search.visit(0, &initial) {
        Some(true) => Verdict::Linearizable(search.order),
        Some(false) => Verdict::NonLinearizable,
        None => Verdict::BudgetExhausted,
    })
}

struct Search<'a, I, O, S, F> {
    history: &'a [Operation<I, O>],
    predecessors: Vec<u64>,
    explored: HashSet<(u64, S)>,
    order: Vec<usize>,
    max_states: usize,
    transition: F,
}

impl<I, O, S: Clone + Eq + Hash, F: FnMut(&S, &I, &O) -> Vec<S>> Search<'_, I, O, S, F> {
    fn visit(&mut self, done: u64, state: &S) -> Option<bool> {
        if self.order.len() == self.history.len() {
            return Some(true);
        }
        let key = (done, state.clone());
        if self.explored.contains(&key) {
            return Some(false);
        }
        if self.explored.len() == self.max_states {
            return None;
        }
        self.explored.insert(key);
        for (index, op) in self.history.iter().enumerate() {
            let bit = 1 << index;
            if done & bit != 0 || self.predecessors[index] & !done != 0 {
                continue;
            }
            for next in (self.transition)(state, &op.input, &op.output) {
                self.order.push(index);
                if self.visit(done | bit, &next)? {
                    return Some(true);
                }
                self.order.pop();
            }
        }
        Some(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn op(invoked: u64, finished: u64, input: i32, output: i32) -> Operation<i32, i32> {
        Operation {
            invoked,
            finished,
            input,
            output,
        }
    }

    fn add(state: &i32, input: &i32, output: &i32) -> Vec<i32> {
        let next = state + input;
        if next == *output {
            vec![next]
        } else {
            vec![]
        }
    }

    #[test]
    fn overlapping_operations_may_reorder_but_nonoverlapping_ones_cannot() {
        let concurrent = [op(1, 3, 0, 1), op(2, 4, 1, 1)];
        assert_eq!(
            linearize(&concurrent, 0, 10, add),
            Ok(Verdict::Linearizable(vec![1, 0]))
        );
        let serial = [op(1, 2, 0, 1), op(3, 4, 1, 1)];
        assert_eq!(linearize(&serial, 0, 10, add), Ok(Verdict::NonLinearizable));
        assert_eq!(linearize(&serial, 0, 0, add), Ok(Verdict::BudgetExhausted));
    }

    #[test]
    fn allowed_indeterminate_effect_is_constrained_by_later_observation() {
        let history = [op(1, 2, 1, -1), op(3, 4, 0, 1)];
        assert_eq!(
            linearize(&history, 0, 10, |state, input, output| {
                if *output == -1 {
                    vec![*state, state + input]
                } else {
                    add(state, input, output)
                }
            }),
            Ok(Verdict::Linearizable(vec![0, 1]))
        );
    }
}
