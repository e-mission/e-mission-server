import unittest
import datascience 
import prob140
import numpy as np
import warnings

class TestProb140Methods(unittest.TestCase):
    def setUp(self):
        self.states = ['A', 'B', 'C', 'D'] 
        self.transition_matrix = [
            np.random.dirichlet(np.ones(len(self.states)), size=1)[0],
            np.random.dirichlet(np.ones(len(self.states)), size=1)[0],
            np.random.dirichlet(np.ones(len(self.states)), size=1)[0],
            np.random.dirichlet(np.ones(len(self.states)), size=1)[0]
        ] 
        self.initial_state = 'A'

    def test_create_markov_chain(self):
        table = prob140.MarkovChain.from_matrix(self.states, self.transition_matrix)
        print(table)

    def test_take_step(self):
        mc = prob140.MarkovChain.from_matrix(self.states, self.transition_matrix)
        next_state = mc.simulate_path(self.initial_state, 1)[-1]
        print(next_state)
        next_state = mc.simulate_path(next_state, 1)[-1]
        print(next_state)

if __name__ == '__main__':
    unittest.main()