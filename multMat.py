import pywren_ibm_cloud as pywren
import numpy as np
import sys
import pickle
import marshal

MAX_WORKERS = 100   # numero de workers maxim

numWorkers = 1

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
               
        files = np.append(files, np.random.randint(10, size=(rang, n)))     # crear files
        numFiles += rang

        partSer = marshal.dumps(files.astype(int))
        #partSer = pickle.dumps(files, pickle.HIGHEST_PROTOCOL)          # serialitzar i guardar
        ibm_cos.put_object(Bucket=bucket, Key='A'+str(i), Body=partSer)
    
    # Inicialitzar i guardar matriu B
    numCol = 0
    col = []
    for i in range(numWorkers):
        if (i < numWorkers - 1):            # trobar quantes columnes crear
            rang = int(l/numWorkers)
        else:
            rang = l - numCol
             
        col = np.append(col, np.random.randint(10, size=(rang,n)))         # crear columnes 
        numCol += rang

        partSer = marshal.dumps(col.astype(int))
        #partSer = pickle.dumps(col, pickle.HIGHEST_PROTOCOL)            # serialitzar i guardar
        ibm_cos.put_object(Bucket=bucket, Key='B'+str(i), Body=partSer)

    return [files.astype(int), col.astype(int)]

    

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


def test(bucket, ibm_cos):
    chunkASer = ibm_cos.get_object(Bucket=bucket, Key='A0')['Body'].read()
    chunkBSer = ibm_cos.get_object(Bucket=bucket, Key='B0')['Body'].read()
    chunkA = marshal.loads(chunkASer)
    chunkB = marshal.loads(chunkBSer)
    print(str(chunkA)+'\t'+str(chunkB))
    #return [chunkA, chunkB]


if __name__ == '__main__':
    try:
        numWorkers = int(sys.argv[1])
        if numWorkers > 0:
            if numWorkers <= MAX_WORKERS:
                if numWorkers <= m*n:
                    ibmcf = pywren.ibm_cf_executor()
                    ibmcf.call_async(inicialitzar, 'sd-python')
                    interdata = ['sd-python']
                    ibmcf.call_async(test, 'sd-python')
                    '''
                    if(numWorkers == 1):
                        ibmcf.map(multiplicar, interdata)
                    else:
                        ibmcf.map_reduce(multiplicar, interdata, reduir)
                    '''
                    print(ibmcf.get_result())
                    
                    ibmcf.clean()
                else:
                    print("El nombre de workers ha de ser inferior a "+str(m*n))
            else:
                print("El nombre de workers ha de ser inferior a 100")
        else:
            print("El nombre de workers ha de ser superior a 0")
    except IndexError:
        print("S'ha d'indicar el nombre de workers")
    
    
    



#def funct (par, par, ibm_cos)  # ibm_cos fa una instancia del cos

