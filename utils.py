# -*- coding: utf-8 -*-
import collections
import copy
import datetime
import functools
import hashlib
import json
import os
import typing
# import pandas as pd
import re
import pathlib
import logging
import traceback

from typing import Dict, List

# 日志信息结构
_LOG_FORMAT_1 = '%(asctime)s  %(levelname)s--> %(message)s <--%(filename)s - %(funcName)s - %(lineno)d'
_LOG_FORMAT_2 = '%(asctime)s [%(filename)s -> %(funcName)s -> %(lineno)d] %(levelname)s: %(message)s'
# 初始化日志配置
logging.basicConfig(level=logging.INFO, format=_LOG_FORMAT_2)
my_logger = logging.getLogger(__name__)


def local_data_2_list(path: str, mapping_dict: typing.Dict) -> typing.List:
    """
    获取本地excel文件数据转成dict
    :param mapping_dict: 键映射
    :param path: 文件路径
    :return:
    """
    ret_list = []
    # 获取文件数据
    df = pd.read_excel(io=path)
    # 空值处理
    df = df.fillna('')
    # 遍历数据
    for index, row in df.iterrows():
        # 数据追加
        ret_list.append({k: row[v] for k, v in mapping_dict.items()})
    return ret_list


def data_filter(data_list: typing.List, rule_def_list: typing.List[type]) -> typing.List:
    """
    数据筛选方法
    :param data_list: 数据列表
    :param rule_def_list: 规则
    :return:
    """
    ret_list = []
    # 列表判空
    if not data_list:
        return ret_list
    # 获取数据
    for data in data_list:
        # 校验
        _bool = data_verify(data=data, rule_def_list=rule_def_list.copy())
        if _bool:
            ret_list.append(data)  # 追加数据
    return ret_list  # 数据结束


def data_verify(data: Dict, rule_def_list: List[type]) -> bool:
    """
    递归数据校验
    :param data: 被校验数据
    :param rule_def_list: 规则方法列表
    :return: true|false
    """
    # 列表判空
    if not rule_def_list:
        return True
    # 获取规则
    _def = rule_def_list.pop(0)
    # 执行
    ret = _def(data)

    if not ret:
        return False  # 规则未通过
    elif not rule_def_list:
        return ret  # 规则结束
    else:
        return data_verify(data=data, rule_def_list=rule_def_list)  # 递归


def get_all_appoint_file_path(base_path: str, file_name_re: re,
                              call_back_def: typing.Callable[[str], bool] = None) -> typing.List:
    """
    获取路径下所有有效指定文件
    :param call_back_def: 回调函数
    :param base_path: 基础路径
    :param file_name_re: 配置文件名称规则
    :return:
    """
    # path_list = []  # 文件地址列表
    # file_name_list = os.listdir(base_path)  # 获取路径下所有文件名称
    # # 遍历文件名
    # for _name in file_name_list:
    #     new_path = os.path.join(base_path, _name)  # 拼接路径
    #     # 判断是否文件
    #     if os.path.isfile(path=new_path):
    #         # 判断文件名称是否符合规则
    #         if re.match(file_name_re, _name):
    #             # 执行回调函数
    #             if call_back_def and not call_back_def(new_path):
    #                 continue
    #             path_list.append(new_path)  # 追加地址
    #         else:
    #             pass
    #             # print(f"不是配置文件 {_name}")
    #     else:
    #         path_list += get_all_appoint_file_path(base_path=new_path, file_name_re=file_name_re,
    #                                                call_back_def=call_back_def)  # 递归
    # return path_list

    return [i for i in get_all_appoint_file_path_yield(base_path=base_path, file_name_re=file_name_re,
                                                       call_back_def=call_back_def)]


def get_all_appoint_file_path_yield(base_path: str, file_name_re: re,
                                    call_back_def: typing.Callable[[str], bool] = None) -> typing.Generator:
    """
    获取路径下所有有效指定文件 迭代器
    :param call_back_def: 回调函数
    :param base_path: 基础路径
    :param file_name_re: 配置文件名称规则
    :return:
    """
    # 遍历文件夹下所有文件路径
    for _path_obj in pathlib.Path(base_path).iterdir():
        _base_path = str(_path_obj.absolute())
        # 判断是否文件
        if _path_obj.is_file():
            # 判断文件名称是否符合规则
            if re.match(file_name_re, _path_obj.name):
                # 执行回调函数
                if call_back_def and not call_back_def(_base_path):
                    continue
                yield _base_path  # 追加地址
            else:
                pass
        else:
            for _path in get_all_appoint_file_path_yield(base_path=_base_path, file_name_re=file_name_re,
                                                         call_back_def=call_back_def):  # 递归
                yield _path


def remove_null_file(base_path: str) -> None:
    """
    清除空文件夹
    :param base_path: 基础路径
    :return:
    """
    file_name_list = os.listdir(base_path)  # 获取路径下所有文件名称
    # 遍历文件名
    for _name in file_name_list:
        new_path = os.path.join(base_path, _name)  # 拼接路径
        # 判断是否文件
        if not os.path.isfile(path=new_path):
            # 判断是否为空
            if not os.listdir(path=new_path):
                os.rmdir(new_path)
            else:
                remove_null_file(base_path=new_path)


def remove_appoint_file(base_path: str, file_name_re: re, debug: bool = True) -> None:
    """
    清除指定文件名文件
    :param debug: 是否调试
    :param file_name_re: 文件规则
    :param base_path: 基础路径
    :return:
    """
    # 获取所有符合条件的路径
    file_path_list = get_all_appoint_file_path(base_path=base_path, file_name_re=file_name_re)
    # 遍历路径
    for _path in file_path_list:
        # 判断是否调试
        if debug:
            print(f"调试 {_path}")
        else:
            os.remove(_path)  # 删除文件
            print(f"删除成功 {_path}")


def appoint_data_initialize(base_dict: typing.Dict, source_dict: typing.Dict, rule_list: typing.List) -> typing.Dict:
    """
    特定字段初始化
    :param rule_list: 初始化规则 [{'base_key_str': 'k1->k2', 'source_key_str': 'k1->k2', 'rule_def': lambda}]
    :param base_dict: 基础数据
    :param source_dict: 源数据
    :return:
    """
    # 遍历规则列表
    for _rule_dict in rule_list:
        _base_key_str = _rule_dict.get('base_key_str', '')  # 获取基础数据键结构
        _source_key_str = _rule_dict.get('source_key_str', '')  # 获取源数据键结构
        _rule_def = _rule_dict.get('rule_def', lambda d: not d)  # 获取初始化判断规则
        _base_data = get_obj_data(base_data=base_dict, key_str=_base_key_str)  # 获取数据
        _source_data = get_obj_data(base_data=source_dict, key_str=_source_key_str)  # 获取数据
        # 判断是否符合条件  修改数据值
        if _rule_def(_base_data):
            base_dict = set_obj_data(base_data=copy.deepcopy(base_dict), target_data=copy.deepcopy(_source_data),
                                     key_str=_base_key_str)
    return base_dict


def set_obj_data(base_data: typing.Union[typing.Dict, typing.List], target_data: typing.Any, key_str: str,
                 separator: str = r'->',
                 key_list: typing.List = None) -> typing.Any:
    """
    设置对象指定结构下的数据
    :param key_list: 键列表
    :param target_data: 目标数据
    :param base_data: 数据源
    :param key_str: 结构
    :param separator: 结构分隔符
    :return:
    """
    # 生成键列表
    if not key_list:
        key_list = re.split(separator, key_str)
    _key, _key_list = key_list[0], key_list[1:]  # 切割列表
    # 判断数据源是否有键
    if not isinstance(base_data, dict):
        my_logger.info(f"数据类型错误 数据：{base_data} 键：{_key}")
        return None
    else:
        if _key not in base_data:
            my_logger.info(f"键错误 数据：{base_data} 键：{_key}")
            return None
    # 判断键列表的长度
    if len(key_list) > 1:
        base_data[_key] = set_obj_data(base_data=base_data[_key], target_data=target_data,
                                       key_str=key_str, key_list=_key_list)
    else:
        base_data[_key] = target_data  # 赋值
    return base_data  # 返回


def get_obj_data(base_data: typing.Union[typing.Dict, typing.List], key_str: str, separator: str = r'->') -> typing.Any:
    """
    获取对象指定结构下的数据
    :param base_data: 数据源
    :param key_str: 结构
    :param separator: 结构分隔符
    :return:
    """
    ret_data = base_data  # 返回数据初始化
    # 遍历基础数据键结构 获取数据
    for _key in re.split(separator, key_str):
        # 判断数据是否有效
        if not ret_data:
            my_logger.info(f"数据无效 数据源：{base_data} 结构：{key_str} 分隔符：{separator}")
            return None
        ret_data = ret_data.get(_key)  # 获取数据
    return ret_data  # 返回


def data_assignment(base_obj: typing.Union[typing.Dict, typing.List],
                    source_obj: typing.Union[typing.Dict, typing.List]) -> typing.Union[typing.Dict, typing.List]:
    """
    数据赋值
    :param base_obj: 基础数据字典
    :param source_obj: 源数据字典
    :return:
    """
    # 判断基础字典是否为空
    if base_obj:
        if isinstance(base_obj, dict):
            _keys = base_obj.keys()  # 获取字典键
            _condition = lambda _a: _a in _keys  # 条件
        elif isinstance(base_obj, list):
            _base_length = len(base_obj)  # 获取长度
            _source_length = len(source_obj)  # 获取长度
            _length = max(_base_length, _source_length)  # 获取最大长度
            _keys = range(_length)  # 下标集合
            _condition = lambda _a: _a < _source_length  # 条件
            # 判断模板数据长度是否小于最大长度
            if _base_length < _length:
                # 遍历而外长度
                for _ in range(_length - _base_length):
                    base_obj.append('')  # 追加占位符
        else:
            _keys = []
            _condition = lambda _a: False
        # 遍历基础数据键
        for _key in _keys:
            # 判断源数据是否有数据键
            if _condition(_key):
                # 判断数据是否有效
                if _key not in source_obj:
                    continue
                _source_value = source_obj[_key]  # 获取源数据
                if isinstance(_source_value, dict) or isinstance(_source_value, list):
                    base_obj[_key] = data_assignment(base_obj=base_obj[_key], source_obj=_source_value)
                else:
                    base_obj[_key] = _source_value  # 数据复制
    else:
        base_obj = source_obj  # 数据复制
    return base_obj


def get_all_list(source_obj: typing.Any) -> typing.List:
    """
    获取对象中的所有列表集合
    :param source_obj:
    :return:
    """
    ret_list = []  # 返回的列表
    # 判断是否字典
    if isinstance(source_obj, dict):
        # 遍历字典
        for _, _value in source_obj.items():
            _ret = get_all_list(source_obj=_value)  # 递归
            # 判断返回值是否有效
            if _ret:
                ret_list += _ret  # 数据累加
    # 判断是否列表
    elif isinstance(source_obj, list):
        ret_list = source_obj  # 记录列表
    else:
        pass
    return ret_list  # 数据返回


def get_path_file_info(file_path: str) -> typing.Tuple[str, str, str, str]:
    """
    拆分文件路径
    :param file_path: 文件路径
    :return:
    """
    _base_path, _file_name = os.path.split(file_path)  # 拆分文件路径
    _short_name, _suffix = os.path.splitext(_file_name)  # 拆分文件名
    return _base_path, _file_name, _short_name, _suffix


def list_dict_2_dict_list(data_list: typing.Union[typing.List, typing.Generator],
                          title_mapping: typing.Dict = None) -> typing.Dict:
    """
    字典列表转列表字典 用于data frame类型数据文件导出
    :param title_mapping: 列标题中英文映射
    :param data_list: 源数据列表
    :return:
    """
    if not data_list:
        raise Exception(f"源数据列表（data_list）无效！！！")
    ret_dict = {}
    _keys_dict = []
    # 遍历数据列表 生成字典数据
    for _data_dict in data_list:
        if not ret_dict:
            # 判断是否有列标题映射
            if title_mapping:
                _keys_dict = title_mapping
            else:
                _keys_dict = collections.OrderedDict({_k: _k for _k in dict(_data_dict).keys()})  # 获取字典键
            ret_dict = collections.OrderedDict({})  # 返回的字典对象
            # 字典键 字典初始化
            for _key, _title in _keys_dict.items():
                ret_dict[_title] = []
        # 遍历字典数据键
        for _key, _title in _keys_dict.items():
            _data = _data_dict[_key] if _key in _data_dict else ''  # 获取字典指定数据
            # 判断返回字典是否初始化
            if _title in ret_dict:
                ret_dict[_title].append(_data)  # 数据追加
            else:
                ret_dict[_title] = [_data]  # 数据初始化
    return ret_dict  # 返回


def get_md5(data: typing.Any) -> str:
    """
    获取数据的MD5值
    :param data: 数据源
    :return:
    """
    # print(f"数据源：{data}")
    _md5 = hashlib.md5()
    _str_b = str(data).encode('utf-8')
    _md5.update(_str_b)
    return _md5.hexdigest()


def insert_index(num_list: typing.List[int], data: int) -> int:
    """
    返回数字插入的位置
    :param num_list: 数字列表
    :param data: 插入的值
    :return: 应插入的位置
    """
    # 特殊情况
    if data > num_list[len(num_list) - 1]:
        return len(num_list)
    _left = 0  # 左索引
    _right = len(num_list) - 1  # 右索引
    while _left < _right:
        _middle = int(_left + (_right - _left) / 2)
        if num_list[_middle] == data:
            return _middle
        elif num_list[_middle] < data:
            _left = _middle + 1
        else:
            _right = _middle
    return _right


def get_bracket_group_index(data: str, index_type: str = '') -> typing.List:
    """
    获取括号组 括号字符串的下标
    :param data:
    :param index_type: 下标类型 '' outer
    :return:
    """
    # 括号字典
    _bracket_dict = {
        '(': ')',  # en
        '（': '）',  # zh

        '\[': '\]',  # en
        '【': '】',  # zh
        '［': '］',  # 全角
        '〖': '〗',  #

        '{': '}',  # 大括号
        '｛': '｝',  # 全角大括号
        '﹝': '﹞',  # 六角
        '〔': '〕',  # 全角六角

        '«': '»',  #
        '‹': '›',  #
        '<': '>',  # en
        '〈': '〉',  #
        '《': '》',  # zh

        '「': '」',  #
        '『': '』',  #
    }
    # 左括号正则
    _left_re = r'[{}]'.format(''.join(list(_bracket_dict.keys())))
    _right_re = r'[{}]'.format(''.join(list(_bracket_dict.values())))
    # 获取所有括号下标
    _left_bracket_iter = re.finditer(_left_re, data)  # 迭代器
    _left_bracket_list = [_i.start() for _i in _left_bracket_iter]  # 下标
    _left_length = len(_left_bracket_list)  # 左括号数量
    _right_bracket_iter = re.finditer(_right_re, data)  # 迭代器
    _right_bracket_list = [_i.start() for _i in _right_bracket_iter]  # 下标
    _right_length = len(_right_bracket_list)  # 右括号数量
    # 获取所有括号分组下标列表  以按左括号下标排序
    ret_list = min_difference(a_list=_left_bracket_list, b_list=_right_bracket_list)
    # 仅返回最外层括号
    if index_type == 'outer':
        # 判断下标列表是否有效
        if ret_list:
            _start_list = [ret_list[0][0]]  # 获取第一组左括号
            _end_list = [ret_list[0][1]]  # 获取第一组右括号
            # 遍历剩余括号组
            for _group in ret_list[1:]:
                # 判断 当前括号组左括号 是否 不被 前面括号包含
                if _group[0] > _end_list[-1]:
                    _start_list.append(_group[0])  # 添加左括号下标
                    _end_list.append(_group[1])  # 添加右括号下标
            return list(zip(_start_list, _end_list))  # 打包转成列表
        else:
            return ret_list
    else:
        return ret_list


def min_difference(a_list: typing.List, b_list: typing.List) -> typing.List:
    """
    计算两个列表的最小正数差值
    :param a_list: 数值列表1
    :param b_list: 数值列表2
    :return:
    """
    # 没有数据 终止
    if len(a_list) == 0 or len(b_list) == 0:
        return []

    ret_dict = {}
    # 遍历b数组
    for _b in b_list:
        for _a in a_list:
            _diff = _b - _a  # 计算差值
            # 如果差值合理
            if _diff > 0:
                # 记录数值组合
                if _diff in ret_dict:
                    ret_dict[_diff].append((_a, _b))
                else:
                    ret_dict[_diff] = [(_a, _b)]
    # 字典键排序
    _key_list = sorted(ret_dict)
    # 获取差值最小的数据组 可能有多个
    ret_list = ret_dict[_key_list[0]]
    # 清除以选中的数据组
    for _a, _b in ret_list:
        a_list.remove(_a)
        b_list.remove(_b)
    # 如果两个列表都有数据
    if len(a_list) > 0 and len(b_list) > 0:
        ret_list += min_difference(a_list=a_list, b_list=b_list)  # 递归
        ret_list.sort(key=lambda _a: _a[0])  # 按a数据排序
        return ret_list
    else:
        ret_list.sort(key=lambda _a: _a[0])  # 按a数据排序
        return ret_list  # 返回最小的额组合


def file_open_verify(file_path: str) -> None:
    """
    文件打开校验
    :param file_path: 文件地址
    :return:
    """
    try:
        with open(file=file_path, mode='w+') as f:
            f.readline()
    except Exception as e:
        raise Exception(f"请关闭文件：{file_path} error：{e}")


def time_statistics(func: typing.Callable) -> typing.Callable:
    """
    执行时间装饰器
    :param func: 被装饰方法
    :return:
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        _start_time = datetime.datetime.now()  # 开始时间
        ret = func(*args, **kwargs)
        _end_time = datetime.datetime.now()  # 结束时间
        _diff_time = _end_time - _start_time  # 时间差
        print(f"执行时间：{_diff_time.seconds} 秒")
        return ret

    return wrapper


def catch_exceptions(func: typing.Callable) -> typing.Callable:
    """
    异常捕获装饰器
    :param func:
    :return:
    """

    @functools.wraps(func)
    def decorator(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            message = f"程序发生异常, 错误信息：\n{traceback.format_exc()}"
            # print(message)
            return message

    return decorator


def format_print(data_list: typing.List, size: int = 5, key: str = '') -> None:
    """
    格式化字符串列表打印
    :return:
    """
    _size = size
    for _index, d in enumerate(data_list):
        if key:
            _message = f"\t{d[key]}"  # 调试语句内容
        else:
            _message = f"\t{d}"  # 调试语句内容
        # 设置输出结尾
        format_print_one(index=_index, size=_size, message=_message)


def format_print_one(index: int, size: int = 5, message: str = "") -> None:
    """
    格式化字符串打印
    :return:
    """
    _size = size
    if not message:
        _message = f"\t{index}"
    else:
        _message = message
    # 设置输出结尾
    if index % _size == _size - 1:
        _end = '\n'
    else:
        _end = ''
    print(_message, end=_end)  # 调试语句


if __name__ == '__main__':
    pass
