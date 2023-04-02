import os


class Logger:

    def __init__(self, dir, fn, overwrite=True):
        if not os.path.exists(dir):
            os.mkdir(dir)
        self.path = f"{dir}/{fn}"

        if overwrite:
            with open(self.path, 'w') as f:
                f.write("")

    def write_str(self, s):
        with open(self.path, 'a') as f:
            f.write(f"{s}\n")

    def write_dict(self, d, label=None):
        with open(self.path, 'a') as f:
            if label is not None:
                f.write(f"{label}\n")
            for key in d:
                f.write(f"{key}: {d[key]}\n")

    def write_list(self, l, label=None):
        with open(self.path, 'a') as f:
            if label is not None:
                f.write(f"{label}\n")
            for i in l:
                f.write(f"{i}\n")

