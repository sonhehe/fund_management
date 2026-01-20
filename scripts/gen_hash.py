import bcrypt


pw = "admin123"  
hashed = bcrypt.hashpw(pw.encode(), bcrypt.gensalt())
print(hashed.decode())

