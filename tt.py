full_tenure = 120
rds_tenure = 18
amt = 100000
ret_amt = 100000
total_principal = 0
total_interest = 0
total = 0
total_investment = 0
for i in range(1,120):
    total_investment += amt
    total_principal += amt
    
    if i%18 == 0:
        print(i)
       
        
    if i > 18:
        
        total_principal =  total_principal + ret_amt 
        

print(total_investment, "Total Principle", total_principal)
