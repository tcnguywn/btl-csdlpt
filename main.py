from function import getopenconnection, loadratings, test_loadratings, rangepartition, roundrobinpartition, roundrobininsert

conn = getopenconnection("postgres", "ROOT", "postgres")
print("get connection success")
# loadratings('ratings', r"E:\DefaultFeature\Downloads\bai_tap_lon_CSDL_phan_tan-main\bai_tap_lon_CSDL_phan_tan-main\test_data.dat", conn)
loadratings('ratings', r"E:\DefaultFeature\Downloads\ml-10m\ml-10M100K\ratings.dat", conn)
print("load successful")
# test_loadratings()

# print("\n=== Testing Range Partitition Functions ===")
# rangepartition('ratings', 3, conn)

print("\n=== Testing Round Robin Functions ===")

# Tạo 5 partitions
roundrobinpartition('ratings', 3, conn)
print("new partitions created")
# # Test insert
# roundrobininsert('ratings', 9999, 1001, 4.5, conn)
# print("new record inserted")

# # Đóng connection
# conn.close()

