import pywren_ibm_cloud as pywren
import numpy as np
import marshal

matriuA = []
matriuB = []
matriuC = []
m = 2
n = 2
l = 2

def inicialitzar(bucket, ibm_cos):
    matriuA = np.random.randint(10, size=(m,n))
    matA = marshal.dumps(matriuA)
    ibm_cos.put_object(bucket, "matA", matA)

    matriuB = np.random.randint(10, size=(n,l))
    matB = marshal.dumps(matriuB)
    ibm_cos.put_object(bucket, "matB", matB)
    
def multiplicar(bucket, ibm_cos):
    matA = marshal.loads(ibm_cos.get_object(bucket, "matA"))
    matB = marshal.loads(ibm_cos.get_object(bucket, "matB"))
    matriuC = matA.dot(matB)
    return matriuC

if __name__ == '__main__':
    ibmcf = pywren.ibm_cf_executor()
    ibmcf.call_async(inicialitzar, 'sd-python')
    mat = [matriuA, matriuB]
    print(matriuA)
    print(matriuB)
    ibmcf.call_async(multiplicar, 'sd-python')
    print(ibmcf.get_result())
    



#def funct (par, par, ibm_cos)  # ibm_cos fa una instancia del cos

