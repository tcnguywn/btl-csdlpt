from function import getopenconnection, loadratings, roundrobinpartition, roundrobininsert

conn = getopenconnection("postgres", "root", "db_assign1")
print("get connection success")
# loadratings('ratings', r"D:\Giao trinh\csdl phan tan\bai_tap_lon_CSDL_phan_tan\ml-10m\ml-10M100K\ratings.dat", conn)
# print("load successful")


# Thêm các dòng này để test round robin
print("\n=== Testing Round Robin Functions ===")

# Tạo 5 partitions
roundrobinpartition('ratings', 5, conn)
print("new partitions created")
# Test insert
roundrobininsert('ratings', 9999, 1001, 4.5, conn)
print("new record inserted")

# Đóng connection
conn.close()

