import os
import time
import logging
from pydantic import Field
from typing import Dict, List, Any, Optional

from proconfig.widgets.base import WIDGETS, BaseWidget
from apify_client import ApifyClient

@WIDGETS.register_module()
class ApifyKVStoreWidget(BaseWidget):
    """
    Apify KV存储操作Widget，可以上传或下载键值数据。
    需要设置环境变量APIFY_API_KEY。
    """
    CATEGORY = "Custom Widgets/Data Tools"
    NAME = "Apify KV Store"
    
    class InputsSchema(BaseWidget.InputsSchema):
        operation: str = Field("download", description="操作类型 (upload/download)")
        store_name: str = Field("default-store", description="KV存储名称")
        value: Optional[dict] = Field(None, description="上传时的值（JSON对象）")
        max_items: int = Field(10, description="下载时返回的最大项目数")
        
    class OutputsSchema(BaseWidget.OutputsSchema):
        success: bool = Field(description="操作是否成功")
        message: str = Field(description="状态消息")
        data: Optional[Dict[str, Any]] = Field(None, description="下载的数据")
    
    def execute(self, environ, config):
        """
        执行Apify KV存储操作
        
        Args:
            environ: 环境变量
            config: 配置参数
        
        Returns:
            包含操作结果的字典
        """
        try:
            # 从环境变量获取Apify API密钥
            apify_api_key = os.environ.get("APIFY_API_KEY")
            
            # 如果环境变量中没有API密钥，返回错误
            if not apify_api_key:
                return {
                    "success": False,
                    "message": "缺少APIFY_API_KEY环境变量，请设置后再试",
                    "data": None
                }
            
            # 创建Apify客户端
            client = ApifyClient(apify_api_key)
            
            # 获取或创建KV存储
            store = client.key_value_stores().get_or_create(name=config.store_name)
            store_id = store["id"]
            
            # 根据操作类型执行不同的逻辑
            if config.operation.lower() == "upload":
                if not config.value:
                    return {
                        "success": False,
                        "message": "上传操作需要提供值",
                        "data": None
                    }
                
                # 使用当前时间戳作为键
                timestamp = str(int(time.time()))
                
                # 存储值
                client.key_value_store(store_id).set_record(timestamp, config.value)
                
                return {
                    "success": True,
                    "message": f"成功上传数据，键为 {timestamp}",
                    "data": {timestamp: config.value}
                }
                
            elif config.operation.lower() == "download":
                # 获取存储中的所有键
                list_result = client.key_value_store(store_id).list_keys()
                keys = [item["key"] for item in list_result["items"] if item["key"].isdigit()]
                
                # 按时间戳排序（降序，最新的在前）
                keys.sort(reverse=True)
                
                # 获取最新的n个记录
                latest_keys = keys[:config.max_items]
                result_data = {}
                
                for key in latest_keys:
                    record = client.key_value_store(store_id).get_record(key)
                    if record and "value" in record:
                        result_data[key] = record["value"]
                
                return {
                    "success": True,
                    "message": f"成功获取最新的 {len(result_data)} 条记录",
                    "data": result_data
                }
                
            else:
                return {
                    "success": False,
                    "message": f"不支持的操作类型: {config.operation}",
                    "data": None
                }
                
        except Exception as e:
            # 记录错误并处理
            logging.error(f"Apify KV存储操作失败: {repr(e)}")
            return {
                "success": False,
                "message": f"操作失败: {repr(e)}",
                "data": None
            }


if __name__ == "__main__":
    # 必须在运行前设置环境变量
    os.environ["APIFY_API_KEY"] = ""  # 填入你的API密钥
    
    widget = ApifyKVStoreWidget()
    
    # 测试上传
    upload_config = {
        "operation": "upload",
        "store_name": "test-store",
        "value": {"test": "data", "time": "now"},
        "max_items": 10
    }
    
    upload_result = widget({}, upload_config)
    print("上传结果:", upload_result)
    
    # 测试下载
    download_config = {
        "operation": "download",
        "store_name": "test-store",
        "value": None,
        "max_items": 2
    }
    
    download_result = widget({}, download_config)
    print("下载结果:", download_result)
    import datetime
    # 将时间戳转换回日期时间并打印
    if download_result["success"] and download_result["data"]:
        print("\n时间戳转换:")
        for timestamp, value in download_result["data"].items():
            dt = datetime.datetime.fromtimestamp(int(timestamp))  # 直接转换秒级时间戳
            formatted_time = dt.strftime("%Y-%m-%d %H:%M:%S")
            print(f"时间戳 {timestamp} = {formatted_time}")
            print(f"数据: {value}")
            print("-" * 30)
