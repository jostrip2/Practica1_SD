import pywren_ibm_cloud as pywren
import numpy as np
import sys
import pickle

MAX_WORKERS = 100   # numero de workers maxim


m = 4           # files de la matriu A
n = 4           # columnes de la matriu A // files de la matriu B
l = 6           # columnes de la matriu B

numWorkers = 1

def inicialitzar(bucket, ibm_cos):
    matA = np.random.randint(10, size=(m, n))
    matB = np.random.randint(10, size=(n, l))
    
    if (numWorkers < m):     # Dividir matriu A
        partA = int(m/numWorkers)
        iniciA = 0
        finalA = 0
        for i in range(numWorkers):
            iniciA = i * partA              # trobar quines files corresponen a cada chunk
            if (i == numWorkers - 1):            
                finalA = m
            else:
                finalA = (i+1) * partA
            
            chunkA = matA[iniciA:finalA]
            partSer = pickle.dumps(chunkA)          # serialitzar i guardar
            ibm_cos.put_object(Bucket=bucket, Key='A'+str(i+1), Body=partSer)
    else:
        for i in range(m):          # separem cada fila
            chunkA = matA[i]
            chunkA = np.reshape(chunkA,(1,n))
            partSer = pickle.dumps(chunkA)          # serialitzar i guardar
            ibm_cos.put_object(Bucket=bucket, Key='A'+str(i+1), Body=partSer)
    
    if (numWorkers < l):    # Dividir matriu B
        partB = int(l/numWorkers)
        iniciB = 0
        finalB = 0
        for i in range(numWorkers):
            iniciB = i * partB              # trobar quines columnes corresponen a cada chunk
            if (i == numWorkers - 1):            
                finalB = l
            else:
                finalB = (i+1) * partB
            
            chunkB = matB[:, iniciB:finalB]
            partSer = pickle.dumps(chunkB)          # serialitzar i guardar
            ibm_cos.put_object(Bucket=bucket, Key='B'+str(i+1), Body=partSer)
    else:
        for i in range(l):          # separem cada columna
            chunkB = matB[:, i]
            chunkB = np.reshape(chunkB,(n,-1))
            partSer = pickle.dumps(chunkB)          # serialitzar i guardar
            ibm_cos.put_object(Bucket=bucket, Key='B'+str(i+1), Body=partSer)

    return [matA, matB]

    

def multiplicar(files, col, bucket, ibm_cos, id):
    
    matA = [] 
    chunksA = []
    chunksB = []
    matB = []
    if numWorkers == 1:
        chunkASer = ibm_cos.get_object(Bucket=bucket, Key='A1')['Body'].read()      # desserialitzar matriu A
        matA = pickle.loads(chunkASer)
        chunkBSer = ibm_cos.get_object(Bucket=bucket, Key='B1')['Body'].read()      # desserialitzar matriu B
        matB = pickle.loads(chunkBSer)

        chunkC = np.matmul(matA, matB)          # multiplicar les matrius

        partSer = pickle.dumps(chunkC)          # serialitzar i guardar
        ibm_cos.put_object(Bucket=bucket, Key='C'+str(id+1), Body=partSer)

        return chunkC

    elif numWorkers <= m or numWorkers <= l:
        primer = True
        for i in files:
            chunkASer = ibm_cos.get_object(Bucket=bucket, Key='A'+str(i))['Body'].read()    # obtenir i desserialitzar chunks de A
            chunksA = pickle.loads(chunkASer)
            if primer:              # si és el primer chunk
                matA = chunksA
                primer = False
            else:                   # els següents chunks es concatenen a la següent fila
                matA = np.concatenate((matA, chunksA), axis=0)
        
        primer = True
        for j in col:
            chunkBSer = ibm_cos.get_object(Bucket=bucket, Key='B'+str(j))['Body'].read()    # obtenir i desserialitzar chunks de B
            chunksB = pickle.loads(chunkBSer)
            if primer:              # si és el primer chunk
                matB = chunksB
                primer = False
            else:                   # els següents chunks es concatenen a la següent columna
                matB = np.concatenate((matB, chunksB), axis=1)
    
        chunkC = np.matmul(matA, matB)      # multiplicar els chunks

        partSer = pickle.dumps(chunkC)            # serialitzar i guardar
        ibm_cos.put_object(Bucket=bucket, Key='C'+str(id+1), Body=partSer)
        
        return chunkC

    else:
        retorn = []     # paràmetres per la funció de reduir
        for i in range(len(files)):
            chunkASer = ibm_cos.get_object(Bucket=bucket, Key='A'+str(files[i]))['Body'].read()     # obtenir i desserialitzar fila
            matA = pickle.loads(chunkASer)
            chunkBSer = ibm_cos.get_object(Bucket=bucket, Key='B'+str(col[i]))['Body'].read()       # obtenir i desserialitzar columna
            matB = pickle.loads(chunkBSer)

            chunkC = np.dot(matA, matB)                 # multiplicar la fila per la columna
            retorn.append([[files[i],col[i]], chunkC])  # afegeix la posició del valor i el valor
            partSer = pickle.dumps(chunkC)              # serialitzar i guardar
            ibm_cos.put_object(Bucket=bucket, Key='C'+str(files[i])+'-'+str(col[i]), Body=partSer)
    
        return retorn
        



def reduir(results):
    if numWorkers <= m or numWorkers <= l:
        primer = True
        for result in results:
            if primer:
                matC = result
                primer = False
            else:
                matC = np.append(matC, result)
        matC = np.reshape(matC, (m, l))
    else:
        matC = np.empty((m,l), np.int64)
        for result in results:
            for elem in result:
                i = elem[0][0] - 1
                j = elem[0][1] - 1
                val = elem[1]
                matC[i][j] = val
    
    return matC


if __name__ == '__main__':
    
    while True:
        print("\nIndicar el nombre de workers: ")
        val = input()
        try:
            numWorkers = int(val)
            if (numWorkers <= 0 or numWorkers > MAX_WORKERS or numWorkers > m*l):
                print("El nombre de workers ha de ser entre 1 i "+str(MAX_WORKERS))
        except ValueError:
            print("El valor introduit NO es correcte")
        else:
            break

    print("\nIndicar el nom del bucket: ")
    val = input()
    ibmcf = pywren.ibm_cf_executor()
    params = {'bucket':val}
    ibmcf.call_async(inicialitzar, params)
        
    
    ibmcf.wait()
    if(numWorkers == 1):
        interdata = [dict(files=1, col=1)]
        ibmcf.map(multiplicar, interdata, extra_params=params)
    else:
        interdata = []
        if numWorkers <= m:             # repartim els chunks de la matriu C separant per files (mantenim la matriu B sencera)
            if numWorkers <= l:
                rang = numWorkers + 1
            else:
                rang = l + 1
            for i in range(numWorkers):
                interdata.append(dict(files=[i+1], col=list(range(1, rang, 1))))

        elif numWorkers <= l:           # repartim els chunks de la matriu C separant per columnes (mantenim la matriu A sencera)
            for i in range(numWorkers):
                interdata.append(dict(files=list(range(1, m+1, 1)), col=[i+1]))

        else:   #numWorkers > m,l --->  repartim les posicions de la matriu C entre els workers
            fila = 1        # fila de la matriu C
            col = 1         # columna de la matriu C
            w = 1           # numero del worker
            valors = []     # vector on es guardaran quantes posicion de la matriu C ha de calcular el worker

            for _ in range(m*l):
                if len(valors) < w:     # si el worker no te cap posicio assignada
                    valors.append(dict(files=[fila], col=[col]))
                else:                   # quan el worker ja te alguna posicio assignada
                    elem = valors[w-1]
                    elem['files'].append(fila)      # afegim la nova fila i columna necessaria per fer el calcul de la posicio 
                    elem['col'].append(col)
                
                col += 1                # control de la fila i la columna
                if col > l:
                    fila += 1
                    col = 1

                w += 1                  # quan ja hem repartit a tots els workers, tornem a assignar al primer
                if w > numWorkers:
                    w = 1

            interdata = valors.copy()   # copiem les posicions assignades a cada worker a la variable interdata         

        ibmcf.map_reduce(multiplicar, interdata, reduir, extra_params=params)
    
    
    result = ibmcf.get_result()     # obtenim els resultats 

    matA = result[0][0]             # mostrem la matriu A
    print("Matriu A: ")
    print(matA)
    print()

    matB = result[0][1]             # mostrem la matriu B
    print("Matriu B: ")
    print(matB)
    print()
    
    matC = result[1]                # mostrem la matriu C
    print("Matriu C: ")
    print(matC)
    print()
    
    ibmcf.clean()
    