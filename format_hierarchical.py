import pickle

from io import StringIO
from rich.tree import Tree
from rich.console import Console

ROOT = "/programs-surveys/acs"
DATA_FILE = "acs_structure.pkl"
OUT = "acs_structure.txt"


def build_tree(data):
    tree = Tree("FTP Root")
    nodes = {ROOT: tree}

    for root, (_, files) in data.items():
        if root not in nodes:
            paths = root.split("/")
            parent = nodes["/".join(paths[:-1])]
            nodes[root] = parent.add(paths[-1])

        for file in files:
            nodes[root].add(file)

    return tree


if __name__ == "__main__":

    with open(DATA_FILE, "rb") as f:
        data = pickle.load(f)

    tree = build_tree(data)

    io = StringIO()
    console = Console(file=io)
    console.print(tree)
    output = io.getvalue()

    with open(OUT, "w") as f:
        f.write(output)
