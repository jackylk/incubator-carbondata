#ifndef USER_FORMULA_CPU_H
#define USER_FORMULA_CPU_H

#include "algorithm_plugin_common.h"

extern "C" {
#pragma pack(8)

    /********************************************************************************************************
    函数功能	用原始向量/长特征计算距离
    输入参数
        handle			算法句柄
        stBaseVec		特征库向量集合，地址为内存的地址
        stQueryVec		待查询向量集合，地址为内存的地址
    输出参数
        stDistances		距离计算结果集合，地址为内存地址，结果集合的地址空间由平台申请。
                        每个空间大小为：stBaseVec的向量个数*sizeof（float）
    返回值
        执行结果        0：成功
                        其他失败
    ********************************************************************************************************/
    typedef int(*Plu_Host_CalcDistanceSet_t)(const handle_t handle,
        const Plu_VecSet_S& stBaseVec,
        const Plu_VecSet_S& stQueryVec,
        const Plu_Distance_Set_S& stDistances); 

	/********************************************************************************************************
	函数功能	用原始向量/长特征计算距离
	输入参数
	handle			算法句柄
	stBaseVec		特征库向量集合，地址为内存的地址
	stQueryVec		待查询向量集合，地址为内存的地址
	paraInfo        参数信息  格式:  版本号;修正参数1;修正参数2  如  0.4;0.000,0.446,0.496,0.825,0.909,0.923,0.943,1.00;-1.00,0.135,0.248,0.341,0.421,0.501,0.728,1.00
	输出参数
	stDistances		距离计算结果集合，地址为内存地址，结果集合的地址空间由平台申请。
	每个空间大小为：stBaseVec的向量个数*sizeof（float）
	返回值
	执行结果        0：成功
	其他失败
	********************************************************************************************************/
	typedef int(*Plu_Host_CalcDistanceSetEx_t)(const handle_t handle,
		const Plu_VecSet_S& stBaseVec,
		const Plu_VecSet_S& stQueryVec,
		const Plu_Distance_Set_S& stDistances,
		const char* paraInfo);

    /********************************************************************************************************
    函数功能	用原始向量/长特征计算距离,只计算bitmap中指定向量
    输入参数
        handle			算法句柄
        stBaseVec		特征库向量集合，地址为内存的地址
        stBitMap		特征库的bitmap，用户指定在特征库中需要查询的向量
        stQueryVec		待查询向量集合，地址为内存的地址
    输出参数
        stDistances		距离计算结果集合，地址为内存地址，结果集合的地址空间由平台申请。
                        每个空间大小为：bitmap指定的向量个数*sizeof（float）
    返回值
        执行结果        0：成功
                        其他失败
    ********************************************************************************************************/
    typedef int(*Plu_Host_CalcDistanceSetWithBitMap_t)(const handle_t handle,
        const Plu_VecSet_S& stBaseVec,
        const Plu_Vec_BitMap_Set_S& stBitMap,
        const Plu_VecSet_S& stQueryVec,
        const Plu_Distance_Set_S& stDistances);

	/********************************************************************************************************
	函数功能	用原始向量/长特征计算距离,只计算bitmap中指定向量
	输入参数
	handle			算法句柄
	stBaseVec		特征库向量集合，地址为内存的地址
	stBitMap		特征库的bitmap，用户指定在特征库中需要查询的向量
	stQueryVec		待查询向量集合，地址为内存的地址
	paraInfo        参数信息  格式:  版本号;修正参数1;修正参数2  如  0.4;0.000,0.446,0.496,0.825,0.909,0.923,0.943,1.00;-1.00,0.135,0.248,0.341,0.421,0.501,0.728,1.00
	输出参数
	stDistances		距离计算结果集合，地址为内存地址，结果集合的地址空间由平台申请。
	每个空间大小为：bitmap指定的向量个数*sizeof（float）
	返回值
	执行结果        0：成功
	其他失败
	********************************************************************************************************/
	typedef int(*Plu_Host_CalcDistanceSetWithBitMapEx_t)(const handle_t handle,
		const Plu_VecSet_S& stBaseVec,
		const Plu_Vec_BitMap_Set_S& stBitMap,
		const Plu_VecSet_S& stQueryVec,
		const Plu_Distance_Set_S& stDistances,
		const char* paraInfo);


    /********************************************************************************************************
    函数功能	用原始向量/长特征计算距离
    输入参数
        handle			算法句柄
        stBaseVec		特征库向量列表，地址为内存的地址
        stQueryVec		待查询向量集合，地址为内存的地址
    输出参数
        stDistances		距离计算结果集合，地址为内存地址，结果集合的地址空间由平台申请。
                        每个空间大小为：stBaseVec的向量个数*sizeof（float）
    返回值
        执行结果        0：成功
                        其他失败
    ********************************************************************************************************/
    typedef int(*Plu_Host_CalcDistanceList_t)(const handle_t handle,
        const Plu_VecList_S& stBaseVec,
        const Plu_VecSet_S& stQueryVec,
        const Plu_Distance_Set_S& stDistances);

	/********************************************************************************************************
	函数功能	用原始向量/长特征计算距离
	输入参数
	handle			算法句柄
	stBaseVec		特征库向量列表，地址为内存的地址
	stQueryVec		待查询向量集合，地址为内存的地址
	paraInfo        参数信息  格式:  版本号;修正参数1;修正参数2  如  0.4;0.000,0.446,0.496,0.825,0.909,0.923,0.943,1.00;-1.00,0.135,0.248,0.341,0.421,0.501,0.728,1.00
	输出参数
	stDistances		距离计算结果集合，地址为内存地址，结果集合的地址空间由平台申请。
	每个空间大小为：stBaseVec的向量个数*sizeof（float）
	返回值
	执行结果        0：成功
	其他失败
	********************************************************************************************************/
	typedef int(*Plu_Host_CalcDistanceListEx_t)(const handle_t handle,
		const Plu_VecList_S& stBaseVec,
		const Plu_VecSet_S& stQueryVec,
		const Plu_Distance_Set_S& stDistances,
		const char* paraInfo);


    /* 原始特征距离的方法列表 */
    typedef struct
    {
        Plu_Host_CalcDistanceSet_t				pfnCalcDistanceSet;
		Plu_Host_CalcDistanceSetEx_t			pfnCalcDistanceSetEx;
        Plu_Host_CalcDistanceSetWithBitMap_t	pfnCalcDistanceSetWithBitMap;
		Plu_Host_CalcDistanceSetWithBitMapEx_t	pfnCalcDistanceSetWithBitMapEx;
        Plu_Host_CalcDistanceList_t				pfnCalcDistanceList;
		Plu_Host_CalcDistanceListEx_t			pfnCalcDistanceListEx;
    }Plu_Host_CalcDist_Methods_S;


    /**********************************   算法插件需要实现的接口   *********************************************************/
    /********************************************************************************************************
    函数功能		获取算法库的版本
    输入参数
        无
    输出参数
        version     算法插件版本号，返回宏定义 ALGORITHM_PLUGIN_VERSION 的值
    返回值
        执行结果        0：成功
                        其他失败
    ********************************************************************************************************/
    extern int Plu_Host_GetPluginVersion(int* version);
    /********************************************************************************************************
    函数功能		初始化算法库实例
    输入参数
        pcPluginPath    算法库路径,如/opt/algorithm/libpq.so
        stParams		算法库配置的参数
        stHostMethod	主机内存申请方法
        pfnLog          日志打印函数指针
    输出参数
        handle		算法实例句柄，句柄指向的内存用于记录算法库实例的相关信息
    返回值
        执行结果        0：成功
                        其他失败
    ********************************************************************************************************/
    extern int Plu_Host_InitPlugin(const char* pcPluginPath,
        const Plu_Param_Set_S stParams,
        const Srs_Host_MemoryMethods_S stHostMethod,
        const Srs_Log pfnLog,
        handle_t* handle);

    /********************************************************************************************************
    函数功能		注册原始特征的距离计算和搜索的方法
    输入参数
        handle		算法实例句柄
    输出参数
        pstMethods		方法列表
    返回值
        执行结果        0：成功
                        其他失败
    ********************************************************************************************************/
    extern int Plu_Host_RegistSearchMethod(const handle_t handle, Plu_Host_CalcDist_Methods_S* pstMethods);

    /********************************************************************************************************
    函数功能		平台设置原始特征的距离计算和搜索的方法到插件
    输入参数
        handle		算法实例句柄
        pstMethods	 方法列表
        methodsHandle 距离计算的算法实例句柄
        enSortType    距离排序方式
    返回值
        执行结果        0：成功
                        其他失败
    ********************************************************************************************************/
    extern int Plu_Host_SetSearchMethod(const handle_t handle, const Plu_Host_CalcDist_Methods_S pstMethods,
                                        const handle_t methodsHandle, SORT_TYPE_E enSortType); 

    /********************************************************************************************************
    函数功能		清除算法库实例
    输入参数
        handle		算法实例句柄
    输出参数
        无
    返回值
        执行结果    0：成功
                    其他失败
    ********************************************************************************************************/
    extern int Plu_Host_FinalizePlugin(const handle_t handle);

#pragma pack()
}
#endif
