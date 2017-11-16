

class LeaseEntry(object):

    def __init__(self, line=None):
        self.line = line
        self.mac = ''
        self.ip = ''
        self.parse(line)

        def parse(self, line):
            fields = map(str.strip, line.split(' '))
            self.mac = fields[1]
            self.ip = fields[2]


class Leases(object):

    def __init__(self, filepath):
        self.filepath = filepath
        self.reload()

    def reload(self):
        with open(self.filepath, 'rb') as stream:
            self.content = stream.read().split('\n')
        self.content = filter(None, self.content)
        self.entries = self.parse()

    def entries(self):
        for line in self.content:
            if line:
                yield LeaseEntry(line)
