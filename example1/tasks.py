import os
import numpy as np

from six.moves import urllib
import luigi
import law

law.contrib.load("tasks")  # to have the RunOnceTask


class GenerateFile(law.Task):

    def output(self):
        # The file needs to have the full path
        return law.LocalFileTarget('/home/bellis/test_output.txt')

    def run(self):
        n = 10
        data = np.random.random(n)
        print(data)
        with self.output().open('w') as f:
            data.tofile(f, sep=',')
