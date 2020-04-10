import pywren_ibm_cloud as pywren
import numpy as np
import sys
import pickle

MAX_WORKERS = 100   # numero de workers maxim


m = 4           # files de la matriu A
n = 4           # columnes de la matriu A // files de la matriu B
l = 4           # columnes de la matriu B

numWorkers = 1

def inicialitzar(bucket, workers, ibm_cos):
    matA = np.random.randint(10, size=(m, n))
    matB = np.random.randint(10, size=(n, l))
    
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
    else:
        for i in range(m):
            chunkA = matA[i]
            partSer = pickle.dumps(chunkA)          # serialitzar i guardar
            ibm_cos.put_object(Bucket=bucket, Key='A'+str(i+1), Body=partSer)
    

    if (numWorkers < l):    # Dividir matriu B
        partB = int(l/numWorkers)
        iniciB = 0
        finalB = 0
        for i in range(numWorkers):
            iniciB = i * partB
            if (i == numWorkers - 1):            
                finalB = m
            else:
                finalB = (i+1) * partB
            
            chunkB = matB[:, iniciB:finalB]
            partSer = pickle.dumps(chunkB)          # serialitzar i guardar
            ibm_cos.put_object(Bucket=bucket, Key='B'+str(i+1), Body=partSer)
    else:
        for i in range(l):
            chunkB = matB[:, i]
            partSer = pickle.dumps(chunkB)          # serialitzar i guardar
            ibm_cos.put_object(Bucket=bucket, Key='B'+str(i+1), Body=partSer)

    return [matA, matB]

    

def multiplicar(files, col, bucket, ibm_cos):
    
    matA = [] 
    chunksA = []
    chunksB = []
    matB = []
    if numWorkers == 1:
        chunkASer = ibm_cos.get_object(Bucket=bucket, Key='A1')['Body'].read()
        matA = pickle.loads(chunkASer)
        chunkBSer = ibm_cos.get_object(Bucket=bucket, Key='B1')['Body'].read()
        matB = pickle.loads(chunkBSer)

    elif numWorkers <= m or numWorkers <= l:
        # agafar i desserialitzar chunks de A 
        primer = True
        for i in files:
            chunkASer = ibm_cos.get_object(Bucket=bucket, Key='A'+str(i))['Body'].read()
            chunksA = pickle.loads(chunkASer)
            if primer:
                matA = chunksA
                primer = False
            else:
                matA = np.stack((matA, chunksA))
        
        # agafar i desserialitzar chunks de B
        primer = True
        for j in col:
            chunkBSer = ibm_cos.get_object(Bucket=bucket, Key='B'+str(j))['Body'].read()
            chunksB = pickle.loads(chunkBSer)
            chunksB = np.split(chunksB, len(chunksB)/n)
            if primer:
                matB = chunksB
                primer = False
            else:                
                matB = np.hstack((matB, chunksB))
        #matB = np.transpose(matB)
        # TODO falta posar be la matriu 
    '''
    # multiplicar chunks
    chunkC = np.matmul(matA, matB)

    partSer = pickle.dumps(chunkC)            # serialitzar i guardar
    ibm_cos.put_object(Bucket=bucket, Key='C'+str(files)+'1', Body=partSer)
    '''
    return [matA, matB]  



def reduir(results):
    # Agafar totes les parts de C, fer append i fer reshape (m,l) ????
    if numWorkers <= m:
        primer = True
        for result in results:
            if primer:
                matC = result
                primer = False
            else:
                matC = np.append(matC, result)
    elif numWorkers <= l:
        return
    else:
        return

    matC = np.reshape(matC, (m, l))
    return matC



def getMat(bucket, ibm_cos):
    partsC = []
    for i in range(numWorkers):
        part = pickle.loads(ibm_cos.get_object(Bucket=bucket, Key='C'+str(i+1)+'1')['Body'].read())
        for j in part:
            partsC.extend(j)
    partsC = np.array(partsC).reshape(m,l)

    return partsC



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
            ibmcf.wait()
            if(numWorkers == 1):
                interdata = [dict(files=1, col=1)]
                ibmcf.map(multiplicar, interdata, extra_params={'bucket':'sd-python'})
            else:
                interdata = []
                if numWorkers <= m:          # partir en files
                    if numWorkers <= l:
                        for i in range(numWorkers):
                            interdata.append(dict(files=[i+1], col=list(range(1, numWorkers+1, 1))))
                    else:
                        for i in range(numWorkers):
                            interdata.append(dict(files=[i+1], col=list(range(1, l+1, 1))))

                elif numWorkers <= l:        # partir en col
                    for i in range(numWorkers):
                        interdata.append(dict(files=list(range(1, m+1, 1)), col=i+1))
                else:               # ???
                    print("NSE")

                print("Interdata: "+str(interdata))
                ibmcf.map(multiplicar, interdata, extra_params={'bucket':'sd-python'})
                #ibmcf.map_reduce(multiplicar, interdata, reduir, extra_params={'bucket':'sd-python'})
            
            #ibmcf.call_async(getMat, 'sd-python')
            
            result = ibmcf.get_result()
            print(result)

            matA = result[0][0]
            print("Matriu A: ")
            print(matA)
            print()

            matB = result[0][1]
            print("Matriu B: ")
            print(matB)
            print()
            
            matC = result[1]
            print("Matriu C: ")
            print(matC)
            print()
            
            ibmcf.clean()
        else:
            print("El nombre de workers ha de ser entre 0 i "+str(MAX_WORKERS))