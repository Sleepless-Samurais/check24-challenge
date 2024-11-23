import json

region_range = {}


class Node:

    def __init__(self, n, children):
        self.n = n
        self.children = children

    def get_all_children(self):

        res = []

        if len(self.children) == 0:
            return [self.n]

        for child in self.children:
            res.extend(child.get_all_children())

        return res

    @classmethod
    def load(cls, data):
        cur = data["id"]
        children = []

        for region in data["subregions"]:
            children.append(cls.load(region))

        return Node(cur, children)

    def out(self):
        ids = self.get_all_children()
        if len(ids) == 0:
            region_range[self.n] = (self.n, self.n)
        else:
            region_range[self.n] = (min(ids), max(ids))
        for child in self.children:
            child.out()


with open("region.json", "rt") as fin:
    data = json.load(fin)


Node.load(data).out()

print(region_range)


tmp = [None] * len(region_range)
for k, v in region_range.items():
    tmp[k] = v


print(region_range)


with open("region_array.json", "wt") as fout:
    json.dump(region_range, fout)
