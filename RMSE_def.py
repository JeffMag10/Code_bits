
predicted = [5,25,.75,110]
observed = [3,21, -1.25,12]                      

def sol_RMSE(predicted,observed):
    difference = []
    total = 0
    ele = 0
    N = len(predicted)
    zip_object = zip(predicted,observed)
    for predicted_i, observed_i, in zip_object:
        difference.append((predicted_i - observed_i)**2)
    while(ele < len(difference)):
        total = total + difference[ele]
        ele +=1
    RMSE = (total**.5)/N
    print('RMSE' ,RMSE)
    #return   
sol_RMSE(predicted,observed)        
    


