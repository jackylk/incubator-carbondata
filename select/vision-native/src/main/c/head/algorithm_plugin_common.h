#ifndef ALGORITHM_PLUGIN_COMMON_H
#define ALGORITHM_PLUGIN_COMMON_H

#include <string>
#include <vector>
#include <map>

/* 算法插件的版本定义，平台通过版本号确定调用插件的方式，改版本号插件不能修改 */
#define ALGORITHM_PLUGIN_VERSION  0x00000010

#define RETURN_OK      0
#define RETURN_ERROR  -1


extern "C" {
#pragma pack(8)

    /* 算法库的实例句柄, 句柄指向的内存用于记录算法库实例的相关信息 */
    typedef void*  handle_t;

    /* 申请和释放主机内存的函数指针类型定义 */
    typedef void* (*Srs_Host_MallocMethod_t)(int size);
    typedef void (*Srs_Host_FreeMethod_t)(void* address);

    /* 申请和释放GPU显存的函数指针类型定义 */
    typedef void* (*Srs_Device_MallocMethod_t)(int size, int deviceId);
    typedef void (*Srs_Device_FreeMethod_t)(void* address, int deviceId);
    
    /* 日志打印级别*/
    typedef enum
    {
        DEBUG   = 0,
        INFO    = 1,
        WARN    = 2,
        ERROR   = 3,
        FATAL   = 4,
        LOG_LV_BUTT        
    } Log_level_E;
    /* 日志打印函数 */
    typedef int (* Srs_Log)(Log_level_E level, const char* format,...);  
    
    /* 主机内存的方法结构体定义 */
    typedef struct
    {
        Srs_Host_MallocMethod_t pfnHostMalloc;  // 申请内存的函数指针
        Srs_Host_FreeMethod_t   pfnHostFree;    // 释放内存的函数指针
    }Srs_Host_MemoryMethods_S;

    /* 主机内存的方法结构体定义 */
    typedef struct
    {
        Srs_Device_MallocMethod_t pfnDeviceMalloc;	// 申请显存的函数指针
        Srs_Device_FreeMethod_t   pfnDeviceFree;	// 释放显存的函数指针
    }Srs_Device_MemoryMethods_S;

    /* 向量或编码的大小信息*/
    typedef struct
    {
        int dimNum;				// 向量或编码的维度
        int vectorSize;		    // 向量或编码的字节数
    } Plu_Vec_Size_S;

    /* 向量或编码的集合 */
    typedef struct
    {
        int   number;			// 向量或编码的个数
        void* address;			// 向量或编码数组的起始地址
        Plu_Vec_Size_S vecSize; // 单个向量或编码的大小信息
    } Plu_VecSet_S;

    /* 保存ID和距离的结构 */
    typedef struct
    {
        int number;				// id或距离的个数
        long* id;				// id的起始地址
        float* distance;		// 距离的起始地址
    }Plu_Id_Distance_S;

    /*搜索的结果集 */
    typedef struct
    {
        int resultNum;					// 结果集的个数
        Plu_Id_Distance_S *results;		// 结果集数组的起始地址
    }Plu_Id_Distance_Set_S;

    /* 保存距离的结构 */
    typedef struct
    {
        int number;			// 距离的个数
        float* distance;	// 距离的起始地址
    } Plu_Distance_S;

    /* 距离集合 */
    typedef struct
    {
        int number;					// 集合中距离结构的数量
        Plu_Distance_S* distances;	// 集合中距离数组的起始地址
    } Plu_Distance_Set_S;

    /* 向量或编码的列表 */
    typedef struct
    {
        int   number;			// 向量或编码的个数		
        void** address;			// 指针数组，保存向量或编码的地址指针
        Plu_Vec_Size_S vecSize; // 单个向量或编码的大小信息
    } Plu_VecList_S;

    /* 参数结构 */
    typedef struct 
    {
	    char * name;    // 参数名
	    char * value;   // 参数值
    } Plu_Plugin_Param_S;

     /* 算法插件的参数列表 */
    typedef struct 
    {
	    int number;             // 参数的个数
	    Plu_Plugin_Param_S *param; // 参数数组的其实地址
    } Plu_Param_Set_S;

    /* 内存或显存结构 */
    typedef struct
    {
        void * address;		// 内存或显存地址
        long size;			// 内存或显存大小
    } Plu_Mem_S;

    /* bitmap信息 */
    typedef struct
    {
        int number;				// bitmap的字节数
        unsigned char *bitMap;	// bitmap的地址
    }Plu_Vec_BitMap_S;

    /* bitmap集合 */
    typedef struct
    {
        int number;						// bitmap的个数
        Plu_Vec_BitMap_S *vecBitMap;	/* bitmap信息数组地址 */
    }Plu_Vec_BitMap_Set_S;

    // 相识度排序规则
    typedef enum 
    {
        ORDER_ASC = 0,   	// 表示距离越小相似度越高
        ORDER_DESC = 1,		// 表示距离越大相似度越高
        ORDER_BUTT
    }SORT_TYPE_E;

    /* 距离公式类型定义 */
    typedef enum
    {
        Formula_Specific_Default = 0,       /* for user, starts from 0 */
        Embeded_Formula_Start,              /* embeded formula begin, user formula must in front of it  */
	    Embeded_L2_Float32 = Embeded_Formula_Start,
        Formula_Type_BUTT,
    }Formula_Type_E;

    /* 编码算法类型定义 */
    typedef enum
    {
        Model_Not_Supported    = 0,
        Model_User_XQ,
        Model_User_HR,
        Model_Embedded_Start,       /*  embeded handler begin, user hanler must in front of it */
        Model_Embedded_XQ = Model_Embedded_Start,
        Model_Embedded_HR,
        Model_Handler_Type_BUTT,
    }Model_Handler_Type_E;

     /* 聚类算法类型定义 */
    typedef enum
    {
        Centralize_Not_Supported = 0,       /* for user, starts from 0 */
        Centralize_Embedded_Start,          /* embeded handler begin, user hanler must in front of it */
	    Centralize_Embedded_XMean = Centralize_Embedded_Start,
        Centralize_Type_BUTT
    }Cent_Handler_Type_E;

#pragma pack()
}

#endif // ALGORITHM_PLUGIN_COMMON_H

