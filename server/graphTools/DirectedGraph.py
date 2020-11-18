from collections import defaultdict
import time
  
class DirectedGraph:

    def __init__(self, vertices=[], graph=defaultdict(list)):
        self.vertices = vertices
        self.graph = graph
    
    def add_vertex(self, v):
        if v not in self.vertices:
            self.vertices.append(v)
  
    def add_edge(self,u,v): 
        if u not in self.vertices:
            raise ValueError(f'{u} is not a vertex')
        if v not in self.vertices:
            raise ValueError(f'{v} is not a vertex')
        if v not in self.graph[u]:
            self.graph[u].append(v)

    def remove_edge(self, u, v):
        if u not in self.vertices:
            raise ValueError(f'{u} is not a vertex')
        if v not in self.vertices:
            raise ValueError(f'{v} is not a vertex')
        if v not in self.graph[u]:
            raise ValueError(f'{u},{v} edge does not exist')
        self.graph[u].remove(v)

    def remove_vertex(self, v):
        self.vertices.remove(v)
        self.graph[v] = []
        for k in self.graph.keys():
            if v in self.graph[k]:
                self.graph[k].remove(v)

    def search_path(self, query, target, max_depth, visited, current_path, paths):
        if query == target:
            paths.append(current_path[:])
        if max_depth==0:
            return
        for neighbour in self.graph[query]:
            if not visited[neighbour]:
                visited[neighbour] = True
                current_path.append(neighbour)
                self.search_path(current_path, neighbour, target, max_depth-1, visited, paths)
                current_path.pop()
                visited[neighbour] = False
        return

    def cycles_from(self, query, max_depth=10):
        visited = {v: False for v in self.vertices}
        all_paths = []
        current_path = [query]
        for neighbour in self.graph[query]:
            self.remove_edge(query, neighbour)
            self.search_path(query, neighbour, max_depth-1, visited, current_path, all_paths)
            self.add_edge(query, neighbour)
        return all_paths
