from function import getopenconnection, loadratings, roundrobinpartition, roundrobininsert, rangepartition, rangeinsert

FILE_PATH = r"D:\Giao trinh\csdl phan tan\bai_tap_lon_CSDL_phan_tan\test_data.dat"

conn = getopenconnection("postgres", "root", "db_assign1")
print("get connection success")

# loadratings('ratings', FILE_PATH, conn)
# print("load successful")

rangepartition('ratings', 5, conn)
print("new partitions created")

# # Thêm các dòng này để test round robin
# print("\n=== Testing Round Robin Functions ===")

# Tạo 5 partitions
# roundrobinpartition('ratings', 5, conn)
# print("new partitions created")
# # Test insert
# roundrobininsert('ratings', 9999, 1001, 4.5, conn)
# print("new record inserted")
#
# rangeinsert('ratings', 145, 8, 4, conn)
# print("insert succesfully")



# Đóng connection
conn.close()

