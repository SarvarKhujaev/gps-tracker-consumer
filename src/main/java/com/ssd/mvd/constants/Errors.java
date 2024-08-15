package com.ssd.mvd.constants;

import com.ssd.mvd.inspectors.StringOperations;

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
    },
    PRIMARY_KEYS_NOT_FOUND {
        @Override
        public String translate (
                final String languageType
        ) {
            return switch ( languageType ) {
                case "uz" -> "HATTO";
                case "ru" -> "Primary keys для %s не найдены".formatted( languageType );
                default -> "Primary keys for entity: %s cannot be empty".formatted( languageType );
            };
        }
    },

    WRONG_TYPE_IN_ANNOTATION {
        @Override
        public String translate (
                final String languageType
        ) {
            return switch ( languageType ) {
                case "uz" -> "Mumkin emas %s".formatted( languageType );
                case "ru" -> "Формат %s не допустим".formatted( languageType );
                default -> "Type %s is unacceptable".formatted( languageType );
            };
        }
    },

    FIELD_MIGHT_NOT_BE_EMPTY {
        @Override
        public String translate (
                final String languageType
        ) {
            return switch ( languageType ) {
                case "uz" -> "Mumkin emas %s".formatted( languageType );
                case "ru" -> "Формат %s не допустим".formatted( languageType );
                default -> "Type %s CANNOT BE NULL".formatted( languageType );
            };
        }
    };

    public String translate (
            final String languageType
    ) {
        return switch ( languageType ) {
            case "ru", "uz" -> StringOperations.EMPTY;
            default -> DATA_NOT_FOUND.name();
        };
    }
}
