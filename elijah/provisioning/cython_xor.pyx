cdef char c[4096]
def cython_xor(char *a,char *b):
    cdef int i
    for i in range(4096):
        c[i]=a[i]^b[i]
    return c[:4096]

def cython_xor_vectorised(char *a,char *b):
    cdef int i
    for i in range(512):
        (<unsigned long long *>c)[i]=(<unsigned long long *>a)[i]^(<unsigned long long *>b)[i]
    return c[:4096]
