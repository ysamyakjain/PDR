import datetime
import logging

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from database import DatabaseConnection

app = FastAPI()
logging.basicConfig(level=logging.DEBUG)

class Asset(BaseModel):
    asset_id: str
    asset_type: str
    asset_name: str
    asset_description: str
    asset_location: str
    asset_status: str
    asset_tags: str

class Gateway(BaseModel):
    gateway_id: str
    gateway_name: str
    gateway_description: str
    gateway_location: str
    gateway_coverage_area: str
    gateway_connectivity_info: str
    gateway_status: str
    gateway_firmware_version: str
    gateway_power_source: str
    gateway_config_params: str

asset_tup = (
    "asset_id", "asset_type", "asset_name", "asset_description", "asset_location",
    "asset_status", "asset_tags"
)
gateway_tup = (
    "gateway_id", "gateway_name", "gateway_description", "gateway_location",
    "gateway_coverage_area", "gateway_connectivity_info", "gateway_status",
    "gateway_firmware_version", "gateway_power_source", "gateway_config_params"
)

async def build_json(key_name, values):
    json = []
    for value in values:
        json_dict = {key: item for key, item in zip(key_name, value)}
        json.append(json_dict)
    return json


@app.get("/")
async def homepage():
    return {"Message from Author": "This is Indoor Geofence System"}


##########################################  :Asset Configuration Services and APIs: ##################################################

#● /api/assets (GET) - Retrieve a list of all existing assets.
@app.get("/api/assets")
async def get_all_assets_details():
    try:
        db = DatabaseConnection()
        await db.connect()
        query = "SELECT asset_id, asset_type, asset_name, asset_description, asset_location, asset_status, asset_tags FROM nik_apis.assets;"
        result = db.fetchall_query(query, ())
        logging.info(f"result: {result}")
        if not result:
            return JSONResponse(status_code=200, content={"message": "No assets found"})
        response = await build_json(asset_tup, result)
        return JSONResponse(status_code=200, content= response)
    except Exception as e:
        logging.error(f"Error executing database query: {str(e)}")
        return JSONResponse(status_code=500, content={"message": "Internal Server Error: " + str(e)})
    finally:
        db.close()


#● /api/assets (POST) - Add a new asset configuration.
@app.post("/api/assets")
async def register_new_asset(asset: Asset):
    try:
        db = DatabaseConnection()
        await db.connect()
        check_query = "SELECT 1 FROM nik_apis.assets WHERE asset_id = %s;"
        check_params = (asset.asset_id,)
        check_result = db.fetchone_query(check_query, check_params)
        if check_result is not None:
            return JSONResponse(status_code=400, content={"message": "Asset already exists"})

        query = "INSERT INTO nik_apis.assets (asset_id, asset_type, asset_name, asset_description, asset_location, asset_status, asset_tags, asset_registration_time) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"
        current_time = datetime.datetime.now()
        params = (
        asset.asset_id, asset.asset_type, asset.asset_name, asset.asset_description,
        asset.asset_location, asset.asset_status, asset.asset_tags, current_time
        )
        db.execute_query(query, params)
        return JSONResponse(status_code=201, content={"message": "Asset details added successfully"})
    except Exception as e:
        logging.error(f"Error executing database query: {str(e)}")
        return JSONResponse(status_code=500, content={"message": "Internal Server Error: " + str(e)})
    finally:
        db.close()

#● /api/assets/:id (GET) - Retrieve details of a specific asset.
@app.get("/api/assets/{asset_id}")
async def get_asset_details_by_id(asset_id: str):
    try:
        db = DatabaseConnection()
        await db.connect()
        query = "SELECT asset_id, asset_type, asset_name, asset_description, asset_location, asset_status, asset_tags FROM nik_apis.assets WHERE asset_id = %s;"
        params = (asset_id,)
        result = db.fetchone_query(query, params)
        logging.info(f"get_asset_by_id: {result}")
        if result is None:
            return JSONResponse(status_code=404, content={"message": "Asset details not found"})
        return JSONResponse(status_code=200, content=await build_json(asset_tup, [result]))
    except Exception as e:
        logging.error(f"Error executing database query: {str(e)}")
        return JSONResponse(status_code=500, content={"message": "Internal Server Error: " + str(e)})
    finally:
        db.close()


#● /api/assets/:id (PUT) - Update an existing asset configuration.
@app.put("/api/assets/{asset_id}")
async def update_asset_details(asset_id: str, request: Request):
    try:
        req_json = await request.json()
        logging.info(f"req_json: {req_json}")
        for key in req_json.keys():
            if key not in ( "asset_type", "asset_name", "asset_description", "asset_location",
                            "asset_status", "asset_tags"):
                return JSONResponse(status_code=400, content={"message": "Invalid key in request body"})
        db = DatabaseConnection()
        await db.connect()  
        query = "SELECT 1 FROM nik_apis.assets WHERE asset_id = %s;"
        params = (asset_id,)
        result = db.fetchone_query(query, params)
        if result is None:
            return JSONResponse(status_code=404, content={"message": "Asset details not found"})
        update_query = "UPDATE nik_apis.assets SET "
        update_params = []
        
        for key, value in req_json.items():
            update_query += f"{key} = %s, "
            update_params.append(value)
        
        update_query = update_query.rstrip(", ") + " WHERE asset_id = %s;"
        update_params.append(asset_id)
        logging.info(f"query: {update_query}")
        db.execute_query(update_query, tuple(update_params))
        db.close()
        return JSONResponse(status_code=200, content={"message": "Asset details updated successfully"})
    
    except Exception as e:
        logging.error(f"Error executing database query: {str(e)}")
        return JSONResponse(status_code=500, content={"message": "Internal Server Error: " + str(e)})



#● /api/assets/:id (DELETE) - Delete an existing asset configuration.
@app.delete("/api/assets/{asset_id}")
async def delete_asset_details(asset_id: str):
    try:
        db = DatabaseConnection()
        await db.connect()
        query = "SELECT 1 FROM nik_apis.assets WHERE asset_id = %s;"
        params = (asset_id,)
        result = db.fetchone_query(query, params)
        if not result:
            return JSONResponse(status_code=404, content={"message": "Asset details not found"})
        delete_query = "DELETE FROM nik_apis.assets WHERE asset_id = %s;"
        db.execute_query(delete_query, params)
        return JSONResponse(status_code=200, content={"message": "Asset details deleted successfully"})
    except Exception as e:
        logging.error(f"Error executing database query: {str(e)}")
        return JSONResponse(status_code=500, content={"message": "Internal Server Error: " + str(e)})
    finally:
        db.close()


#● /api/asset-types (GET) - Retrieve a list of available asset types.
@app.get("/api/asset-types")
async def get_all_asset_types():
    try:
        db = DatabaseConnection()
        await db.connect()
        query = "SELECT asset_id, asset_type FROM nik_apis.assets;"
        result = db.fetchall_query(query, ())
        logging.info(f"result: {result}")
        if not result:
            return JSONResponse(status_code=200, content={"message": "No asset types found"})
        response = await build_json(("asset_id", "asset_type",), result)
        return JSONResponse(status_code=200, content= response)
    except Exception as e:
        logging.error(f"Error executing database query: {str(e)}")
        return JSONResponse(status_code=500, content={"message": "Internal Server Error: " + str(e)})
    finally:
        db.close()


# /api/asset-statuses (GET) - Retrieve a list of available asset status.
@app.get("/api/asset-statuses")
async def get_all_asset_statuses():
    try:
        db = DatabaseConnection()
        await db.connect()
        query = "SELECT asset_id, asset_status FROM nik_apis.assets;"
        result = db.fetchall_query(query, ())
        logging.info(f"result: {result}")
        if not result:
            return JSONResponse(status_code=200, content={"message": "No asset statuses found"})
        response = await build_json(("asset_id", "asset_status",), result)
        return JSONResponse(status_code=200, content= response)
    except Exception as e:
        logging.error(f"Error executing database query: {str(e)}")
        return JSONResponse(status_code=500, content={"message": "Internal Server Error: " + str(e)})
    finally:
        db.close()

    
##########################################  Gateway Configuration Services and APIs ##############################################################

# /api/gateways (GET) - Retrieve a list of existing anchor gateways.
@app.get("/api/gateways")
async def get_all_gateway_details():
    try:
        db = DatabaseConnection()
        await db.connect()
        query = "SELECT gateway_id, gateway_name, gateway_description, gateway_location, gateway_coverage_area, gateway_connectivity_info, gateway_status, gateway_firmware_version, gateway_power_source, gateway_config_params FROM nik_apis.gateways;"
        result = db.fetchall_query(query, ())
        logging.info(f"result: {result}")
        if not result:
            return JSONResponse(status_code=200, content={"message": "No gateways found"})
        response = await build_json(gateway_tup, result)
        return JSONResponse(status_code=200, content= response)
    except Exception as e:
        logging.error(f"Error executing database query: {str(e)}")
        return JSONResponse(status_code=500, content={"message": "Internal Server Error: " + str(e)})
    finally:
        db.close()

# /api/gateways (POST) - Add a new anchor gateway configuration.
@app.post("/api/gateways")
async def register_new_gateway(gateway: Gateway):
    try:
        db = DatabaseConnection()
        await db.connect()
        check_query = "SELECT 1 FROM nik_apis.gateways WHERE gateway_id = %s;"
        check_params = (gateway.gateway_id,)
        check_result = db.fetchone_query(check_query, check_params)
        if not check_result:
            return JSONResponse(status_code=400, content={"message": "Gateway already exists"})

        query = "INSERT INTO nik_apis.gateways (gateway_id, gateway_name, gateway_description, gateway_location, gateway_coverage_area, gateway_connectivity_info, gateway_status, gateway_firmware_version, gateway_power_source, gateway_config_params, gateway_registration_time) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
        current_time = datetime.datetime.now()
        params = (
        gateway.gateway_id, gateway.gateway_name, gateway.gateway_description,
        gateway.gateway_location, gateway.gateway_coverage_area, gateway.gateway_connectivity_info,
        gateway.gateway_status, gateway.gateway_firmware_version, gateway.gateway_power_source,
        gateway.gateway_config_params, current_time
        )
        db.execute_query(query, params)
        return JSONResponse(status_code=201, content={"message": "Gateway details added successfully"})
    except Exception as e:
        logging.error(f"Error executing database query: {str(e)}")
        return JSONResponse(status_code=500, content={"message": "Internal Server Error: " + str(e)})
    finally:
        db.close()

# /api/gateways/:id (GET) - Retrieve details of a specific anchor gateway.
@app.get("/api/gateways/{gateway_id}")
async def get_gateway_details_by_id(gateway_id: str):
    try:
        db = DatabaseConnection()
        await db.connect()
        query = "SELECT gateway_id, gateway_name, gateway_description, gateway_location, gateway_coverage_area, gateway_connectivity_info, gateway_status, gateway_firmware_version, gateway_power_source, gateway_config_params FROM nik_apis.gateways WHERE gateway_id = %s;"
        params = (gateway_id,)
        result = db.fetchone_query(query, params)
        logging.info(f"get_gateway_by_id: {result}")
        if not result:
            return JSONResponse(status_code=404, content={"message": "Gateway details not found"})
        return JSONResponse(status_code=200, content=await build_json(gateway_tup, [result]))
    except Exception as e:
        logging.error(f"Error executing database query: {str(e)}")
        return JSONResponse(status_code=500, content={"message": "Internal Server Error: " + str(e)})
    finally:
        db.close()

#/api/gateways/:id (PUT) - Update an existing anchor gateway configuration.
@app.put("/api/gateways/{gateway_id}")
async def update_gateway_details(gateway_id: str, request: Request):
    try:
        req_json = await request.json()
        logging.info(f"req_json: {req_json}")
        for key in req_json.keys():
            if key not in ( "gateway_name", "gateway_description", "gateway_location",
                            "gateway_coverage_area", "gateway_connectivity_info", "gateway_status",
                            "gateway_firmware_version", "gateway_power_source", "gateway_config_params"):
                return JSONResponse(status_code=400, content={"message": "Invalid key in request body"})
        db = DatabaseConnection()
        await db.connect()  
        query = "SELECT 1 FROM nik_apis.gateways WHERE gateway_id = %s;"
        params = (gateway_id,)
        result = db.fetchone_query(query, params)
        if not result:
            return JSONResponse(status_code=404, content={"message": "Gateway details not found"})
        update_query = "UPDATE nik_apis.gateways SET "
        update_params = []
        
        for key, value in req_json.items():
            update_query += f"{key} = %s, "
            update_params.append(value)
        
        update_query = update_query.rstrip(", ") + " WHERE gateway_id = %s;"
        update_params.append(gateway_id)
        logging.info(f"query: {update_query}")
        db.execute_query(update_query, tuple(update_params))
        db.close()
        return JSONResponse(status_code=200, content={"message": "Gateway details updated successfully"})
    
    except Exception as e:
        logging.error(f"Error executing database query: {str(e)}")
        return JSONResponse(status_code=500, content={"message": "Internal Server Error: " + str(e)})
    
#/api/gateways/:id (DELETE) - Remove an anchor gateway configuration.
@app.delete("/api/gateways/{gateway_id}")
async def delete_gateway_details(gateway_id: str):
    try:
        db = DatabaseConnection()
        await db.connect()
        query = "SELECT 1 FROM nik_apis.gateways WHERE gateway_id = %s;"
        params = (gateway_id,)
        result = db.fetchone_query(query, params)
        if not result:
            return JSONResponse(status_code=404, content={"message": "Gateway details not found"})
        delete_query = "DELETE FROM nik_apis.gateways WHERE gateway_id = %s;"
        db.execute_query(delete_query, params)
        return JSONResponse(status_code=200, content={"message": "Gateway details deleted successfully"})
    except Exception as e:
        logging.error(f"Error executing database query: {str(e)}")
        return JSONResponse(status_code=500, content={"message": "Internal Server Error: " + str(e)})
    finally:
        db.close()

# /api/gateway-statuses (GET) - Retrieve a list of available gateway statuses.
@app.get("/api/gateway-statuses")
async def get_all_gateway_statuses():
    try:
        db = DatabaseConnection()
        await db.connect()
        query = "SELECT gateway_id, gateway_status FROM nik_apis.gateways;"
        result = db.fetchall_query(query, ())
        logging.info(f"result: {result}")
        if not result:
            return JSONResponse(status_code=200, content={"message": "No gateway statuses found"})
        response = await build_json(("gateway_id", "gateway_status",), result)
        return JSONResponse(status_code=200, content= response)
    except Exception as e:
        logging.error(f"Error executing database query: {str(e)}")
        return JSONResponse(status_code=500, content={"message": "Internal Server Error: " + str(e)})
    finally:
        db.close()

#/api/gateway-connections (GET) - Retrieve a list of available connectivity method
@app.get("/api/gateway-connections")
async def get_all_gateway_connections():
    try:
        db = DatabaseConnection()
        await db.connect()
        query = "SELECT gateway_id, gateway_connectivity_info FROM nik_apis.gateways;"
        result = db.fetchall_query(query, ())
        logging.info(f"result: {result}")
        if not result:
            return JSONResponse(status_code=200, content={"message": "No gateway connections found"})
        response = await build_json(("gateway_id", "gateway_connectivity_info",), result)
        return JSONResponse(status_code=200, content= response)
    except Exception as e:
        logging.error(f"Error executing database query: {str(e)}")
        return JSONResponse(status_code=500, content={"message": "Internal Server Error: " + str(e)})
    finally:
        db.close()
