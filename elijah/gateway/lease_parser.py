

class LeaseEntry(object):
    """Single lease entry."""

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
    """Parse dnsmasq lease file."""

    def __init__(self, filepath):
        self.filepath = filepath
        self.load()

    def load(self):
        with open(self.filepath, 'rb') as stream:
            self.content = filter(None, stream.read().split('\n'))

    def entries(self):
        for line in self.content:
            if line:
                yield LeaseEntry(line)
