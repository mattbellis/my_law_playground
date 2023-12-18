import os
import numpy as np

from six.moves import urllib
import luigi
import law

law.contrib.load("tasks")  # to have the RunOnceTask


class GenerateFiles(law.Task):

    def output(self):
        # Each file file needs to have the full path
        file1 = law.LocalFileTarget('/home/bellis/test_output_00.txt')
        file2 = law.LocalFileTarget('/home/bellis/test_output_01.txt')
        # Create this collection
        return law.SiblingFileCollection([file1, file2])

    def run(self):
        nfiles = 2
        n = 10
        print()
        print(self.output())
        print()
        
        # To return the individual files, we use the .targets data member
        for file in self.output().targets:
            with file.open('w') as f:
                data = np.random.random(n)
                print(data)
                data.tofile(f, sep=',')


class ReadFiles(law.Task):

    def requires(self):
        return GenerateFiles.req(self)

    def output(self):
        # summed file
        file1 = law.LocalFileTarget('/home/bellis/summed_file.txt')
        return file1

    def run(self):
        total = None
        for i,file in enumerate(GenerateFiles.output(self).targets):
            with file.open('r') as f:
                data = f.read()
                print(data)
                print(type(data))
                values = data.split(',')
                values = np.array(values).astype(float)
                if i==0:
                    total = values
                else:
                    total += values
        outfile = self.output()
        with outfile.open('w') as f:
            total.tofile(f, sep=',')
