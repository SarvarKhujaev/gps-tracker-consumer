package com.ssd.mvd.constants;

public enum Errors {
    DATA_NOT_FOUND {
        @Override
        public String translate (
                final String languageType
        ) {
            return switch ( languageType ) {
                case "ru" -> "НЕ НАЙДЕНО";
                case "uz" -> "TOPILMADI";
                default -> DATA_NOT_FOUND.name();
            };
        }
    },
    SERVICE_WORK_ERROR {
        @Override
        public String translate (
                final String languageType
        ) {
            return switch ( languageType ) {
                case "ru" -> "СЕРВИС НЕ РАБОТАЕТ";
                case "uz" -> "SERVICE ISHLAMAYAPTI";
                default -> SERVICE_WORK_ERROR.name();
            };
        }
    };

    public String translate (
            final String languageType
    ) {
        return switch ( languageType ) {
            case "ru" -> "";
            case "uz" -> "";
            default -> DATA_NOT_FOUND.name();
        };
    }
}
