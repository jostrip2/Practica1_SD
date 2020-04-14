import numpy as np
import sys

m = 4
n = 4
l = 4
w = 2

matA = np.random.randint(10, size=(m, n))
matB = np.random.randint(10, size=(n, l))
'''
print(matA)
print(matB)
print()


# Imprimir files
for fila in matA:
    print(fila)
print()


# Imprimir columnes
for i in range(len(matB[0])):
    for j in range(len(matB)):
        print(matB[j][i])
print()


# Transposar matB
matBTrans = np.transpose(matB)
for fila in matBTrans:
    print(fila)
print()

# Agafar col concreta--> el 0 es la col que es vol agafar; 0:2 agafa col 0 i 1
print(matB[:, 0])
print()


if (w < m):
    # Dividir matriu A
    partA = int(m/w)
    iniciA = 0
    finalA = 0
    for i in range(w):
        iniciA = i * partA
        if (i == w - 1):            
            finalA = m
        else:
            finalA = (i+1) * partA
        
        chunkA = matA[iniciA:finalA]
        print(chunkA)

elif (w == m or w == m*l):
    for i in range(m):
        print(matA[i])
    print()

else:
    print("NSE A")
    

if (w < l):
    # Dividir matriu B
    partB = int(l/w)
    iniciB = 0
    finalB = 0
    for i in range(w):
        iniciB = i * partB
        if (i == w - 1):            
            finalB = m
        else:
            finalB = (i+1) * partB
        
        chunkB = np.array(matB[:, iniciB:finalB])#.reshape(1, n)
        print(chunkB)

elif (w == l or w == m*l):
    for i in range(n):
        print(matB[i])
    print()    

else:
    print("NSE B")

mul = np.matmul(matA, matB)
print(mul)

matA = np.array([[9, 7],[8, 1],[4, 0],[8, 1]])
matB = np.array([[1, 5],[6, 7],[1, 7],[8, 2]])
print(matA)
print(matB)
A = np.hstack((matA, matB))
B = np.array([[1,7],[3,2],[5,4],[6,5]])
A2 = np.hstack((A,B))
print(A2)

for col in B:
    print(col)
    print(col.shape)
    t = np.transpose(np.array(col))
    print(t)
    print(t.shape)
    print()
    #matA = np.stack((matA, col), axis=1)
#print(np.hsplit(A2, w))

a = np.array([1,2,3,4])
b = np.array([[9, 4, 9, 8],[8, 8, 0, 1],[6, 9, 5, 7],[9, 3, 6, 0]])
print(a)
print(b)

c = np.matmul(a,b)
print(c)

a = np.reshape(a, (1, 4))
print(np.shape(a))
print(np.shape(b))


m = 4
n = 4
l = 4
#w = 8
numWorkers = 12
interdata = []
fila = 1
col = 1
w = 1
valors = []
for _ in range(m*l):
    if len(valors) < w:
        valors.append(dict(files=[fila], col=[col]))
    else:
        elem = valors[w-1]
        elem['files'].append(fila)
        elem['col'].append(col)
    
    col += 1
    if col > l:
        fila += 1
        col = 1

    w += 1
    if w > numWorkers:
        w = 1

for elem in valors:
    interdata.append(elem)
    print(elem)
'''

numWorkers = 5
n = 4
l = 6
matB = np.random.randint(10, size=(n, l))
print(matB)

partB = int(l/numWorkers)
iniciB = 0
finalB = 0
for i in range(numWorkers):
    iniciB = i * partB              # quines columnes agafar
    if (i == numWorkers - 1):            
        finalB = l
    else:
        finalB = (i+1) * partB
    
    chunkB = matB[:, iniciB:finalB]
    print(chunkB)
    #chunkB = np.reshape(chunkB,(n,-1))


