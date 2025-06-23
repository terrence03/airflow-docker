from dags.daily_update_gas_station_namelist import update_cpc, update_fpcc, update_moea

if __name__ == "__main__":
    # Test CPC update
    print("Testing CPC update...")
    update_cpc()

    # Test FPCC update
    print("Testing FPCC update...")
    update_fpcc()

    # Test MOEA update
    print("Testing MOEA update...")
    update_moea()

    print("All updates completed.")
