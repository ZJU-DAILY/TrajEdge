import numpy as np
import cvxpy as cp

def MSU(request: list, free):
    respond = []
    distance = [num[2]['distance'] for num in request]
    R = np.array([num[1] for num in request]).reshape(len(request), 1)

    x = cp.Variable((len(request), 1))
    wasteTime = cp.sum(cp.multiply(np.array(distance).reshape(len(request), 1), x))
    emergency = cp.sum((R - x) ** 2)
    lamb = 0.75
    obj = cp.Minimize((1 - lamb) * wasteTime + lamb * emergency)
    sumx = cp.sum(x, axis=0, keepdims=True)
    con = [sumx == free, x <= R, x >= 0]

    prob = cp.Problem(obj, con)
    # prob.solve(solver='GLPK_MI')
    prob.solve(cp.CPLEX)
    print(x.value)
    for i, it in enumerate(x.value):
        respond.append((request[i][0], int(it.item())))
    return respond


