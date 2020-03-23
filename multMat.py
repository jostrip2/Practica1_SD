import pywren_ibm_cloud as pywren
import numpy as np
import sys
import pickle

MAX_WORKERS = 100   # numero de workers maxim

numWorkers = 1

matriuA = []
matriuB = []
matriuC = []
m = 2           # files de la matriu A
n = 2           # columnes de la matriu A // files de la matriu B
l = 2           # columnes de la matriu B



def inicialitzar(bucket, ibm_cos):
    # Inicialitzar i guardar matriu A
    numFiles = 0
    files = []
    for i in range(numWorkers):
        if (i < numWorkers - 1):            # trobar quantes columnes crear
            rang = int(m/numWorkers)
        else:
            rang = m - numFiles

        for j in range(rang):               # crear files
            np.append(files, np.random.randint(10, size=n))
            numFiles += 1

        partSer = pickle.dumps(files, pickle.HIGHEST_PROTOCOL)          # serialitzar i guardar
        ibm_cos.put_object(Bucket=bucket, Key='A'+str(i), Body=partSer)
    
    # Inicialitzar i guardar matriu B
    numCol = 0
    col = []
    for i in range(numWorkers):
        if (i < numWorkers - 1):            # trobar quantes columnes crear
            rang = int(l/numWorkers)
        else:
            rang = l - numCol

        for j in range(rang):               # crear columnes 
            np.append(col, np.random.randint(10, size=n))
            numCol += 1

        partSer = pickle.dumps(col, pickle.HIGHEST_PROTOCOL)            # serialitzar i guardar
        ibm_cos.put_object(Bucket=bucket, Key='B'+str(i), Body=partSer)

    

def multiplicar(bucket, ibm_cos, id):
    # agafar chunk Aid i Bid
    chunkASer = ibm_cos.get_object(Bucket=bucket, Key='A'+str(id))['Body'].read()
    chunkBSer = ibm_cos.get_object(Bucket=bucket, Key='B'+str(id))['Body'].read()

    # desserialitzar chunks
    chunkA = pickle.loads(chunkASer)
    print(chunkA)
    chunkB = pickle.loads(chunkBSer)
    print(chunkB)

    # multiplicar chunks
    chunkC = []
    for i in range(int(len(chunkA)/n)):
        for j in range(int(len(chunkB)/n)):
            suma = np.zeros(1, dtype=int)
            for k in range(n):
                print(k)
                suma += chunkA[i][k] * chunkB[j][k]
            np.append(chunkC, np.array(suma))  

    partSer = pickle.dumps(chunkC, pickle.HIGHEST_PROTOCOL)            # serialitzar i guardar
    ibm_cos.put_object(Bucket=bucket, Key='C'+str(id)+str(id), Body=partSer)
          



#def reduir():


if __name__ == '__main__':
    numWorkers = int(sys.argv[1])
    if numWorkers <= MAX_WORKERS:
        if numWorkers <= m*n:
            ibmcf = pywren.ibm_cf_executor()
            ibmcf.call_async(inicialitzar, 'sd-python')
            interdata = ['sd-python']
            if(numWorkers == 1):
                ibmcf.map(multiplicar, interdata)
            else:
                ibmcf.map_reduce(multiplicar, interdata, reduir)

            print(ibmcf.get_result())
            
            ibmcf.clean()
        else:
            print("El nombre de workers ha de ser inferior a "+str(m*n))
    else:
        print("El nombre de workers ha de ser inferior a 100")
    
    



#def funct (par, par, ibm_cos)  # ibm_cos fa una instancia del cos

