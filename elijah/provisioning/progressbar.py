import sys


class ProgressBar(object):

    def __init__(self, start=0, end=10, width=12, fill='=', blank='.',
            format='[%(fill)s>%(blank)s] %(progress)s%%',
            incremental=True, **kwargs):
        super(ProgressBar, self).__init__()

        self.start = start
        self.end = end
        self.width = width
        self.fill = fill
        self.blank = blank
        self.format = format
        self.incremental = incremental
        self.step = 100 / float(width)  # fix
        self.reset()

    def set_percent(self, percent):
        done_percent = self._get_progress(percent)
        if done_percent > 100:
            done_percent = 100
        self.progress = done_percent
        return self

    def process(self, increment):
        increment = self._get_progress(increment)
        if 100 > self.progress + increment:
            self.progress += increment
        else:
            self.progress = 100
        return self

    def __str__(self):
        progressed = int(self.progress / self.step)  # fix
        fill = progressed * self.fill
        blank = (self.width - progressed) * self.blank
        return self.format % {
            'fill': fill,
            'blank': blank,
            'progress': int(self.progress)
        }

    __repr__ = __str__

    def _get_progress(self, increment):
        return float(increment * 100) / self.end

    def reset(self):
        """Resets the current progress to the start point"""
        self.progress = self._get_progress(self.start)
        return self


class AnimatedProgressBar(ProgressBar):

    def __init__(self, *args, **kwargs):
        super(AnimatedProgressBar, self).__init__(*args, **kwargs)
        self.stdout = kwargs.get('stdout', sys.stdout)

    def show_progress(self):
        if hasattr(self.stdout, 'isatty') and self.stdout.isatty():
            self.stdout.write('\r')
        else:
            self.stdout.write('\n')
        self.stdout.write(str(self))
        self.stdout.flush()

    def finish(self):
        self.stdout.write('\n')
