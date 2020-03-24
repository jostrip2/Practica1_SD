import pywren_ibm_cloud as pywren
#import numpy as np
import sys
import pickle
import random

MAX_WORKERS = 100   # numero de workers maxim


m = 2           # files de la matriu A
n = 2           # columnes de la matriu A // files de la matriu B
l = 2           # columnes de la matriu B



def inicialitzar(bucket, workers, ibm_cos):
    # Inicialitzar i guardar matriu A
    numFiles = 0
    for i in range(workers):
        if (i < workers - 1):            # trobar quantes columnes crear
            rang = int(m/workers)
        else:
            rang = m - numFiles
               
        matA = []
        for j in range(rang):                  # crear files
            matA.append([])
            for _ in range(n):
                matA[j].append(random.randint(0, 10))

        numFiles += rang

        partSer = pickle.dumps(matA)          # serialitzar i guardar
        ibm_cos.put_object(Bucket=bucket, Key='A'+str(i), Body=partSer)
    
    # Inicialitzar i guardar matriu B
    numCol = 0
    for i in range(workers):
        if (i < workers - 1):            # trobar quantes columnes crear
            rang = int(l/workers)
        else:
            rang = l - numCol
                     
        matB = []
        for j in range(n):                   # crear columnes
            matB.append([])
            for _ in range(rang):
                matB[j].append(random.randint(0, 10)) 
        numCol += rang

        partSer = pickle.dumps(matB)            # serialitzar i guardar
        ibm_cos.put_object(Bucket=bucket, Key='B'+str(i), Body=partSer)

    return [matA, matB]

    

def multiplicar(num, bucket, ibm_cos):
    # agafar chunk Aid i Bid
    chunkASer = ibm_cos.get_object(Bucket=bucket, Key='A'+str(num))['Body'].read()
    chunkBSer = ibm_cos.get_object(Bucket=bucket, Key='B'+str(num))['Body'].read()

    # desserialitzar chunks
    chunkA = pickle.loads(chunkASer)
    chunkB = pickle.loads(chunkBSer)

    # multiplicar chunks
    chunkC = []
    i = 0
    for fila in chunkA:
        for col in chunkB:
            suma = 0
            for i in range(len(col)):
                suma += fila[i] * col[i] 
            chunkC.append(suma)
        i += 1  

    partSer = pickle.dumps(chunkC)            # serialitzar i guardar
    ibm_cos.put_object(Bucket=bucket, Key='C'+str(num)+str(num), Body=partSer)
    return [chunkC]
          

#def reduir():


def test(bucket, ibm_cos):
    chunkCSer = ibm_cos.get_object(Bucket=bucket, Key='C00')['Body'].read()
    chunkC = pickle.loads(chunkCSer)
    return chunkC 


if __name__ == '__main__':
    try:
        numWorkers = int(sys.argv[1])
    except IndexError:
        print("S'ha d'indicar el nombre de workers")
    else:
        if numWorkers > 0:
            if numWorkers <= MAX_WORKERS:
                if numWorkers <= m*n:
                    ibmcf = pywren.ibm_cf_executor()
                    params = {'bucket': 'sd-python', 'workers': numWorkers}
                    ibmcf.call_async(inicialitzar, params)
                    interdata = list(range(numWorkers))
                    if(numWorkers == 1):
                        ibmcf.map(multiplicar, interdata, extra_params=['sd-python'])
                    '''
                    else:
                        ibmcf.map_reduce(multiplicar, interdata, reduir)
                    '''
                    #ibmcf.call_async(test, 'sd-python')
                    print(ibmcf.get_result())
                    
                    ibmcf.clean()
                else:
                    print("El nombre de workers ha de ser inferior a "+str(m*n))
            else:
                print("El nombre de workers ha de ser inferior a 100")
        else:
            print("El nombre de workers ha de ser superior a 0")
    
    
    
    



#def funct (par, par, ibm_cos)  # ibm_cos fa una instancia del cos

