import sys
import os
import networkx as nx
from parse_html import Parser, is_valid
from networkx.exception import NetworkXError
from index_constructor import IndexConstructor, load_doc_records, load_file

def pagerank(G, alpha=0.85, 
             max_iter=100, tol=1.0e-6, nstart=None, weight='weight'): 
    """Return the PageRank of the nodes in the graph. 
    """
    if len(G) == 0: 
        return {} 
  
    if not G.is_directed(): 
        D = G.to_directed() 
    else: 
        D = G 
  
    # Create a copy in (right) stochastic form 
    W = nx.stochastic_graph(D, weight=weight) 
    N = W.number_of_nodes() 

  
    # Choose fixed starting vector if not given 
    if nstart is None: 
        x = dict.fromkeys(W, 1.0 / N) 
    else: 
        # Normalized nstart vector 
        s = float(sum(nstart.values())) 
        x = dict((k, v / s) for k, v in nstart.items()) 
  
    # power iteration: make up to max_iter iterations 
    for _ in range(max_iter): 
        xlast = x 
        x = dict.fromkeys(xlast.keys(), 0) 
        for n in x: 
            for nbr in W[n]: 
                # print(W[n], n, nbr)
                x[nbr] += alpha * xlast[n] / len(W[n])
            x[n] += (1.0 - alpha) / N
  
        # check convergence, l1 norm 
        err = sum([abs(x[n] - xlast[n]) for n in x]) 
        if err < N*tol: 
            return x 
    raise NetworkXError('pagerank: power iteration failed to converge '
                        'in %d iterations.' % max_iter) 
                        
if __name__ == '__main__':
    dir = sys.argv[1]
    parser = Parser()
    url2docId = {a[-1]: a[0] for a in load_doc_records('test')}
    G = nx.MultiDiGraph()
    G.add_nodes_from(url2docId.values())

    # print(url2docId)
    for subdir in os.listdir(dir):
        subdir = os.path.join(dir, subdir)
        for fname in os.listdir(subdir):
            fname = os.path.join(subdir, fname)
            url, content = load_file(fname)
            if url not in url2docId:
                continue
            parser.reset(content)
            _, urls = parser.get_anchor()
            G.add_edges_from((url2docId[url], url2docId[dst]) for dst in filter(lambda x: x in url2docId, urls))

    rank = pagerank(G)
    constructor = IndexConstructor('test')
    constructor.update_page_rank(rank)
    # ranks = {a[0]: a[2] for a in load_doc_records('rank_test')}
    # print(sum(ranks.values()))