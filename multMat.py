import pywren_ibm_cloud as pywren
import numpy as np
import sys
import pickle

MAX_WORKERS = 100   # numero de workers maxim


m = 2           # files de la matriu A
n = 2           # columnes de la matriu A // files de la matriu B
l = 2           # columnes de la matriu B

numWorkers = 1

def inicialitzar(bucket, workers, ibm_cos):
    matA = np.random.randint(10, size=(m, n))
    matB = np.random.randint(10, size=(n, l))
    
    miss = []

    if (numWorkers < m):     # Dividir matriu A
        partA = int(m/numWorkers)
        iniciA = 0
        finalA = 0
        for i in range(numWorkers):
            iniciA = i * partA
            if (i == numWorkers - 1):            
                finalA = m
            else:
                finalA = (i+1) * partA
            
            chunkA = matA[iniciA:finalA]
            partSer = pickle.dumps(chunkA)          # serialitzar i guardar
            ibm_cos.put_object(Bucket=bucket, Key='A'+str(i+1), Body=partSer)

    elif (numWorkers == m or numWorkers == m*l):
        for i in range(m):
            chunkA = matA[i]
            partSer = pickle.dumps(chunkA)          # serialitzar i guardar
            ibm_cos.put_object(Bucket=bucket, Key='A'+str(i+1), Body=partSer)
        
    else:
        miss.append("NSE A")
    

    if (numWorkers < l):
        # Dividir matriu B
        partB = int(l/numWorkers)
        iniciB = 0
        finalB = 0
        for i in range(numWorkers):
            iniciB = i * partB
            if (i == numWorkers - 1):            
                finalB = m
            else:
                finalB = (i+1) * partB
            
            chunkB = np.array(matB[:, iniciB:finalB])
            partSer = pickle.dumps(chunkB)          # serialitzar i guardar
            ibm_cos.put_object(Bucket=bucket, Key='B'+str(i+1), Body=partSer)

    elif (numWorkers == l or numWorkers == m*l):
        for i in range(l):
            chunkB = matB[:, i]
            partSer = pickle.dumps(chunkB)          # serialitzar i guardar
            ibm_cos.put_object(Bucket=bucket, Key='B'+str(i+1), Body=partSer)
 
    else:
        miss.append("NSE B")

    return miss

    
def multiplicar(files, col, bucket, ibm_cos):
    
    chunksA = [] 
    if numWorkers == 1:
        chunkASer = ibm_cos.get_object(Bucket=bucket, Key='A1')['Body'].read()
        chunksA = pickle.loads(chunkASer)
        chunkBSer = ibm_cos.get_object(Bucket=bucket, Key='B1')['Body'].read()
        chunksB = pickle.loads(chunkBSer)

    elif numWorkers <= m:
        # agafar i desserialitzar chunks de A 
        chunkASer = ibm_cos.get_object(Bucket=bucket, Key='A'+str(files))['Body'].read()
        chunksA.append(pickle.loads(chunkASer))
        #chunksA = np.array(chunksA).reshape(len(numList[0]),-1)

        # agafar i desserialitzar chunks de B
        chunksB = []
        for i in col:
            chunkBSer = ibm_cos.get_object(Bucket=bucket, Key='B'+str(i))['Body'].read()
            chunksB.append(pickle.loads(chunkBSer))
        #chunksB = np.array(chunksB).reshape(-1,len(numList[1]))

    
        # multiplicar chunks
        chunkC = []
        i = 0
        for fila in chunksA:
            for colum in chunksB:
                suma = np.dot(fila, colum) 
                chunkC.append(suma) 
        chunkC = np.array(chunksB).reshape(-1,len(col))
        
        partSer = pickle.dumps(chunkC)            # serialitzar i guardar
        ibm_cos.put_object(Bucket=bucket, Key='C'+str(files)+'1', Body=partSer)
                  

def reduir():
    # Agafar totes les parts de C, fer append i fer reshape (m,l) ????
    return


def getMat(bucket, ibm_cos):
    partsA = []
    partsB = []
    partsC = []
    for i in range(numWorkers):
        part = pickle.loads(ibm_cos.get_object(Bucket=bucket, Key='A'+str(i+1))['Body'].read())
        for j in part:
            partsA.append(j)
    partsA = np.array(partsA).reshape(m,-1)

    for i in range(numWorkers):
        part = pickle.loads(ibm_cos.get_object(Bucket=bucket, Key='B'+str(i+1))['Body'].read())
        for j in part:
            partsB.append(j)
    partsB = np.array(partsB).reshape(-1,l)

    for i in range(numWorkers):
        part = pickle.loads(ibm_cos.get_object(Bucket=bucket, Key='C'+str(i+1)+'1')['Body'].read())
        for j in part:
            partsC.extend(j)
    partsC = np.array(partsB).reshape(m,l)

    return [partsA, partsB, partsC]


if __name__ == '__main__':
    try:
        numWorkers = int(sys.argv[1])
    except IndexError:
        print("S'ha d'indicar el nombre de workers")
    else:
        if (numWorkers > 0 and numWorkers <= MAX_WORKERS and numWorkers <= m*l):
            ibmcf = pywren.ibm_cf_executor()
            params = {'bucket': 'sd-python', 'workers': numWorkers}
            ibmcf.call_async(inicialitzar, params)
            
            if(numWorkers == 1):
                interdata = [dict(files=1, col=1)]
                ibmcf.map(multiplicar, interdata, extra_params={'bucket':'sd-python'})
            
            else:
                # TODO Calcular interdata
                interdata = []
                if numWorkers <= m:          # partir en files
                    for i in range(numWorkers):
                        interdata.append(dict(files=i+1, col=list(range(1, numWorkers+1, 1))))
                        #interdata.append(list(list(i+1, list(range(1, numWorkers+1, 1)))))
                elif numWorkers <= l:        # partir en col
                    for i in range(numWorkers):
                        interdata.append(list([list(range(1, numWorkers+1, 1)), i+1]))
                else:               # ???
                    print("NSE")
                ibmcf.map(multiplicar, interdata, extra_params={'bucket':'sd-python'})
                #ibmcf.map_reduce(multiplicar, interdata, reduir)
            ibmcf.call_async(getMat, 'sd-python')
            
            result = ibmcf.get_result()
            miss = result[0]
            print("Missatge: "+str(miss))
            
            matA = result[1][0]
            print("Matriu A: ")
            print(matA)
            matB = result[1][1]
            print("Matriu B: ")
            print(matB)
            matC = result[1][2]
            print("Matriu C: ")
            print(matC)
            print("Interdata: "+str(interdata))
            
            ibmcf.clean()
        else:
            print("El nombre de workers ha de ser entre 0 i "+str(MAX_WORKERS))