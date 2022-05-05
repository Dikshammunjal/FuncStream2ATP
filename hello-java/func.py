import io
import os
import json
import oci
import cx_Oracle
from fdk import response
from base64 import b64decode, b64encode


def handler(ctx, data: io.BytesIO=None):
    fnErrors = "No Error"
    print("hello inside handler")
    try:
        signer = oci.auth.signers.get_resource_principals_signer()
        stream_ocid = "ocid1.stream.oc1.ap-mumbai-1.amaaaaaaak7gbriaxcurpg747fy73kvgw2dnsmjwn6ew3eb6ps5xx62vqr5q"
        stream_endpoint = "cell-1.streaming.ap-mumbai-1.oci.oraclecloud.com"
        stream_client = oci.streaming.StreamClient({}, str("https://" + stream_endpoint), signer=signer)

        cursor_details = oci.streaming.models.CreateCursorDetails()
        cursor_details.partition = "0"
        cursor_details.type = "TRIM_HORIZON"
        cursor = stream_client.create_cursor(stream_ocid, cursor_details)
        r = stream_client.get_messages(stream_ocid, cursor.data.value)

        atp_user = "ADMIN"
        atp_password = "Welcome@1234"
        atp_alias = "db202202161222_low"

        connection = cx_Oracle.connect(atp_user, atp_password, atp_alias)
        cursor = connection.cursor()

        if len(r.data):
            for msg in r.data:
                rs = cursor.execute("select iot_data_seq.nextval from dual")
                rows = rs.fetchone()
                new_id = str(rows).replace(',','')
                new_iot_key = str(b64decode(msg.key).decode('utf-8'))
                new_iot_value = str(b64decode(msg.value).decode('utf-8'))
                rs = cursor.execute("insert into iot_data values ({},'{}','{}')".format(new_id, new_iot_key, new_iot_value))
                rs = cursor.execute('COMMIT')

        cursor.close()
        connection.close()        

    except (Exception, ValueError) as ex:
        fnErrors = str(ex) 

    return response.Response(
        ctx, response_data=json.dumps(
            {"message": "Insert IOT_DATA into ATP database (fnErrors={})".format(fnErrors)}),
        headers={"Content-Type": "application/json"}
    )

