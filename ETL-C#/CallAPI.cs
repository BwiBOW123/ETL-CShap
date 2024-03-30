using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;

namespace ETL_C_
{
    internal class CallAPI
    {
        public async Task<dynamic> GetDataaJson(string api)
        {
            try
            {
                // สร้าง HttpClient
                using (HttpClient client = new HttpClient())
                {
                    // ทำ HTTP GET request เพื่อเรียกใช้ API
                    HttpResponseMessage response = await client.GetAsync(api);

                    // ตรวจสอบว่า response สำเร็จหรือไม่
                    if (response.IsSuccessStatusCode)
                    {
                        // อ่านข้อมูล response เป็น string
                        string responseData = await response.Content.ReadAsStringAsync();
                        dynamic jsonObject = JsonConvert.DeserializeObject(responseData);

                        return jsonObject;
                    }
                    else
                    {
                        Console.WriteLine("API request failed: " + response.StatusCode);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error occurred: " + ex.Message);
            }

            return null;
        }
    }
}
