package cn.cavehicle.utils;

public enum DateFormatDefine {


        //通过构造方法的方式为常量进行赋值
        //定义时间格式的常量
        DATE_TIME_FORMAT("yyyy-MM-dd HH:mm:ss"),
        //定义日期格式的常量
        DATE_FORMAT("yyyyMMdd"),
        //定义日期格式的常量
        DATE2_FORMAT("yyyy-MM-dd");
        //定义变量接收常量的参数
        private String format;
        /**
         * 定义构造方法，目的是为了给常量进行赋值
         * @param format
         */
        DateFormatDefine(String format) {
            this.format = format;
        }

        /**
         * 返回常量定义的参数
         * @return
         */
        public String getFormat() {
            return format;
        }


}
