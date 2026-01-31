use std::{
    cell::{Ref, RefCell, RefMut},
    rc::Rc,
};

use anchor_lang::prelude::Pubkey;
use kamino_lending::utils::AnyAccountLoader;

#[derive(Default, Clone)]
pub struct StateWithKey<T> {
    pub key: Pubkey,
    pub state: Rc<RefCell<T>>,
}

impl<T> StateWithKey<T> {
    pub fn new(state: T, key: Pubkey) -> Self {
        Self {
            key,
            state: Rc::new(RefCell::new(state)),
        }
    }
}

impl<T> AnyAccountLoader<'_, T> for StateWithKey<T> {
    fn get_mut(&self) -> anchor_lang::Result<RefMut<T>> {
        Ok(RefMut::map(self.state.borrow_mut(), |state| state))
    }
    fn get(&self) -> anchor_lang::Result<Ref<T>> {
        Ok(Ref::map(self.state.borrow(), |state| state))
    }

    fn get_pubkey(&self) -> Pubkey {
        self.key
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test that demonstrates why we need to save the original state before simulation.
    /// StateWithKey uses Rc<RefCell<T>>, so clone() shares the same underlying data.
    /// Any mutation through borrow_mut() affects all clones.
    #[test]
    fn test_state_with_key_clone_shares_mutation() {
        let key = Pubkey::new_unique();
        let state = StateWithKey::new(vec![1, 2, 3, 4], key);

        // Clone shares the same RefCell
        let cloned = state.clone();

        // Mutate through the original
        state.state.borrow_mut().pop();

        // Clone sees the mutation!
        assert_eq!(*cloned.state.borrow(), vec![1, 2, 3]);
        assert_eq!(cloned.state.borrow().len(), 3);
    }

    /// Test the fix pattern: save original value before mutation
    #[test]
    fn test_save_before_mutation_preserves_original() {
        let key = Pubkey::new_unique();
        let state = StateWithKey::new(vec![1, 2, 3, 4], key);

        // Save original value BEFORE mutation
        let original = state.state.borrow().clone();

        // Mutate the state
        state.state.borrow_mut().pop();

        // Original value preserved
        assert_eq!(original, vec![1, 2, 3, 4]);
        assert_eq!(original.len(), 4);

        // State was mutated
        assert_eq!(*state.state.borrow(), vec![1, 2, 3]);

        // Create new StateWithKey from original for instructions
        let for_instructions = StateWithKey::new(original, key);
        assert_eq!(for_instructions.state.borrow().len(), 4);
    }
}
