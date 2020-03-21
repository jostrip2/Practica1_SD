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
l = 2           # files de la matriu B



def inicialitzar(bucket, ibm_cos):
    # Inicialitzar i guardar matriu A
    numFiles = 0
    files = []
    for i in range(numWorkers):
        if (i < numWorkers - 1):
            rang = m/numWorkers
        else:
            rang = m - numFiles

        for j in range(rang):
            np.append(files, np.random.randint(10, size=n))
            numFiles += 1

        partSer = pickle.dumps(files, pickle.HIGHEST_PROTOCOL)
        ibm_cos.put_object(Bucket=bucket, Key="A"+str(numWorkers), Body=partSer)
    
    # Inicialitzar i guardar matriu B
    numCol = 0
    col = []
    for i in range(numWorkers):
        if (i < numWorkers - 1):
            rang = l/numWorkers
        else:
            rang = l - numCol

        for j in range(rang):
            np.append(col, np.random.randint(10, size=n))
            numCol += 1

        partSer = pickle.dumps(col, pickle.HIGHEST_PROTOCOL)
        ibm_cos.put_object(Bucket=bucket, Key="B"+str(numWorkers), Body=partSer)

    

#def multiplicar(bucket, ibm_cos):


#def reduir():


if __name__ == '__main__':
    numWorkers = int(sys.argv[1])
    if numWorkers <= MAX_WORKERS:
        if numWorkers <= m*n:
            ibmcf = pywren.ibm_cf_executor()
            ibmcf.call_async(inicialitzar, 'sd-python')
            '''
            if(numWorkers == 1):
                interdata = []
                ibmcf.map(multiplicar, interdata)
            else:
                interdata = []
                ibmcf.map_reduce(multiplicar, interdata, reduir)
            '''
            print(ibmcf.get_result())
            
            ibmcf.clean()
        else:
            print("El nombre de workers ha de ser inferior a "+str(m*n))
    else:
        print("El nombre de workers ha de ser inferior a 100")
    
    



#def funct (par, par, ibm_cos)  # ibm_cos fa una instancia del cos

