import pandas as pd
import os
from pathlib import Path
from datetime import datetime
import logging

class ProductMatcher:
    def __init__(self, products_file, registry_file, price_adjustment_limit=0.02):
        """
        Инициализация сопоставителя товаров и реестров.
        """
        self.products_file = products_file
        self.registry_file = registry_file
        self.price_adjustment_limit = price_adjustment_limit
        self.products_df = None
        self.registry_df = None
        self.remaining_products = None
        self.logger = None
        
    def setup_logging(self, log_dir='logs'):
        """
        Настройка логирования в файл.
        """
        # Создаем директорию для логов
        Path(log_dir).mkdir(exist_ok=True)
        
        # Имя файла лога с временной меткой
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = os.path.join(log_dir, f"matching_{timestamp}.log")
        
        # Настройка логгера
        self.logger = logging.getLogger('ProductMatcher')
        self.logger.setLevel(logging.DEBUG)
        
        # Очищаем старые хендлеры, если есть
        if self.logger.handlers:
            self.logger.handlers.clear()
        
        # Формат логов
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        
        # Хендлер для файла
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)
        
        # Хендлер для консоли (только важные сообщения)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
        
        self.logger.info(f"Логирование запущено. Файл лога: {log_file}")
        return log_file
    
    def load_data(self, products_columns=None, registry_columns=None):
        """
        Загрузка данных из Excel файлов с сохранением всех исходных колонок.
        """
        print("Загружаем данные...")
        
        # Настраиваем логирование при первой загрузке
        if self.logger is None:
            self.setup_logging()
        
        # Значения по умолчанию для обязательных колонок
        if products_columns is None:
            products_columns = {
                'brand': 'Бренд',
                'sku': 'Артикул',
                'quantity': 'Кол-во',
                'price': 'Цена за 1 шт'
            }
        
        if registry_columns is None:
            registry_columns = {
                'description': 'Пояснение',
                'amount': 'Сумма'
            }
        
        # Загружаем товары с сохранением всех колонок
        self.products_df = pd.read_excel(self.products_file)
        
        # Сохраняем все исходные названия колонок
        self.original_columns = self.products_df.columns.tolist()
        self.logger.info(f"Загружены колонки товаров: {', '.join(self.original_columns)}")
        
        # Переименовываем только ключевые колонки для внутреннего использования
        column_mapping = {}
        for key, value in products_columns.items():
            if value in self.products_df.columns:
                column_mapping[value] = key
        
        # Создаем словарь для обратного переименования (для сохранения)
        self.reverse_column_mapping = {v: k for k, v in column_mapping.items()}
        
        # Переименовываем для внутреннего использования
        self.products_df.rename(columns=column_mapping, inplace=True)
        
        # Проверяем, что все необходимые колонки есть
        required_cols = ['sku', 'quantity', 'price']
        missing_cols = [col for col in required_cols if col not in self.products_df.columns]
        if missing_cols:
            raise ValueError(f"В файле товаров отсутствуют обязательные колонки: {missing_cols}")
        
        # Добавляем служебные колонки
        self.products_df['total'] = self.products_df['quantity'] * self.products_df['price']
        self.products_df['original_order'] = range(len(self.products_df))
        
        # Остаток товаров
        self.remaining_products = self.products_df.copy()
        
        # Загружаем реестр
        self.registry_df = pd.read_excel(self.registry_file)
        
        # Переименовываем колонки реестра
        registry_mapping = {}
        for key, value in registry_columns.items():
            if value in self.registry_df.columns:
                registry_mapping[value] = key
        
        self.registry_df.rename(columns=registry_mapping, inplace=True)
        
        # Проверяем, что все необходимые колонки реестра есть
        if 'amount' not in self.registry_df.columns:
            raise ValueError("В файле реестра отсутствует колонка с суммой")
        if 'description' not in self.registry_df.columns:
            raise ValueError("В файле реестра отсутствует колонка с пояснением")
        
        # Логируем статистику
        self.logger.info(f"Загружено товаров: {len(self.products_df)} позиций")
        self.logger.info(f"Загружено реестров: {len(self.registry_df)} записей")
        self.logger.info(f"Общая сумма товаров: {self.products_df['total'].sum():.2f}")
        self.logger.info(f"Общая сумма реестров: {self.registry_df['amount'].sum():.2f}")
        
        # Выводим в консоль краткую информацию
        print(f"✅ Загружено товаров: {len(self.products_df)} позиций")
        print(f"✅ Загружено реестров: {len(self.registry_df)} записей")
        print(f"📊 Подробный лог сохраняется в файл")
    
    def prepare_output_dataframe(self, selected_products):
        """
        Подготавливает DataFrame для сохранения, восстанавливая оригинальные названия колонок.
        Удаляет служебные колонки и оставляет только нужные.
        """
        if not selected_products:  # если список пуст
            return pd.DataFrame()
            
        df = pd.DataFrame(selected_products)
        
        # Восстанавливаем оригинальные названия колонок
        for internal_name, original_name in self.reverse_column_mapping.items():
            if internal_name in df.columns:
                df.rename(columns={internal_name: original_name}, inplace=True)
        
        # Удаляем служебные колонки (total, original_order, price_adjusted)
        service_columns = ['total', 'original_order', 'price_adjusted']
        for col in service_columns:
            if col in df.columns:
                df.drop(col, axis=1, inplace=True)
        
        # Добавляем колонку с суммой по позиции, если её нет
        if 'Сумма по позиции' not in df.columns:
            # Ищем колонки с ценой и количеством
            price_col = None
            qty_col = None
            
            for col in df.columns:
                if 'цена' in col.lower() or 'price' in col.lower():
                    price_col = col
                if 'кол-во' in col.lower() or 'quantity' in col.lower() or 'количество' in col.lower():
                    qty_col = col
            
            if price_col and qty_col:
                df['Сумма по позиции'] = df[qty_col] * df[price_col]
        
        return df
    
    def find_most_expensive_product(self, products_list):
        """
        Находит самый дорогой товар в списке выбранных.
        Возвращает индекс и словарь с данными товара (не Series!)
        """
        if not products_list:
            return None, None
        
        # Преобразуем Series в dict для безопасной работы
        products_dicts = []
        for p in products_list:
            if isinstance(p, pd.Series):
                products_dicts.append(p.to_dict())
            else:
                products_dicts.append(p)
        
        # Ищем товар с максимальной ценой за единицу
        if products_dicts:
            max_price_idx = max(range(len(products_dicts)), 
                               key=lambda i: products_dicts[i]['price'])
            return max_price_idx, products_dicts[max_price_idx]
        
        return None, None
    
    def match_registry(self, target_amount, registry_description, registry_index, tolerance=0.01):
        """
        Подбор товаров под конкретную сумму из реестра.
        """
        selected_products = []
        current_sum = 0
        used_indices = []
        
        # Копия остатка для прохода
        temp_remaining = self.remaining_products.copy()
        
        self.logger.info(f"\n--- Реестр #{registry_index + 1}: {registry_description} ---")
        self.logger.info(f"Целевая сумма: {target_amount:.2f}")
        
        # Основной подбор товаров (идем по порядку)
        for idx, product in temp_remaining.iterrows():
            if current_sum >= target_amount - tolerance:
                break
                
            remaining_needed = target_amount - current_sum
            
            # Для целых штук: максимальное количество, которое можно взять
            if product['price'] <= remaining_needed + tolerance:
                max_qty = int(min(product['quantity'], remaining_needed // product['price']))
                
                if max_qty > 0:
                    # Создаем копию как словарь, чтобы избежать проблем с Series
                    product_copy = product.to_dict()
                    product_copy['quantity'] = max_qty
                    product_copy['total'] = max_qty * product_copy['price']
                    product_copy['price_adjusted'] = False
                    
                    selected_products.append(product_copy)
                    current_sum += product_copy['total']
                    used_indices.append(idx)
                    
                    self.logger.debug(f"  Взят товар: {product_copy['sku']}, {max_qty} шт, сумма: {product_copy['total']:.2f}")
        
        # Проверяем, нужно ли корректировать цену
        diff = target_amount - current_sum
        final_diff = abs(diff)
        
        if final_diff > tolerance and selected_products:
            expensive_idx, expensive_product = self.find_most_expensive_product(selected_products)
            
            # Проверяем, что expensive_product не None и это словарь
            if expensive_product is not None and isinstance(expensive_product, dict):
                original_price = expensive_product['price']
                quantity = expensive_product['quantity']
                
                price_adjustment = diff / quantity
                new_price = original_price + price_adjustment
                
                max_adjustment = original_price * self.price_adjustment_limit
                
                if abs(price_adjustment) <= max_adjustment:
                    # Обновляем цену в словаре
                    expensive_product['price'] = new_price
                    expensive_product['total'] = quantity * new_price
                    expensive_product['price_adjusted'] = True
                    
                    # Обновляем в списке selected_products
                    selected_products[expensive_idx] = expensive_product
                    
                    # Пересчитываем общую сумму
                    current_sum = sum(p['total'] for p in selected_products)
                    
                    self.logger.info(f"  🏷️ Скорректирована цена самого дорогого товара {expensive_product['sku']}:")
                    self.logger.info(f"     {original_price:.2f} → {new_price:.2f} (изменение: {price_adjustment:+.2f})")
                else:
                    self.logger.warning(f"  ⚠️ Не удалось скорректировать цену: требуется {price_adjustment:.2f}, макс. {max_adjustment:.2f}")
            else:
                self.logger.warning("  ⚠️ Не найден дорогой товар для корректировки")
        
        # Финальная проверка
        final_diff = abs(target_amount - current_sum)
        if final_diff <= tolerance:
            self.logger.info(f"  ✅ УСПЕШНО! Сумма: {current_sum:.2f} (цель: {target_amount:.2f})")
            
            # Создаем DataFrame из выбранных товаров
            result_df = pd.DataFrame(selected_products)
            
            # Восстанавливаем исходный порядок
            if 'original_order' in result_df.columns:
                result_df.sort_values('original_order', inplace=True)
            
            # Обновляем остаток
            self._update_remaining_products(used_indices, selected_products)
            
            return result_df, current_sum
        else:
            self.logger.error(f"  ❌ НЕУДАЧА! Получено: {current_sum:.2f}, нужно: {target_amount:.2f}, разница: {final_diff:.2f}")
            return None, current_sum
    
    def _update_remaining_products(self, used_indices, selected_products):
        """
        Обновление остатка товаров после использования.
        """
        used_quantities = {}
        for product in selected_products:
            sku = product['sku']
            used_quantities[sku] = used_quantities.get(sku, 0) + product['quantity']
        
        indices_to_drop = []
        
        for idx, row in self.remaining_products.iterrows():
            sku = row['sku']
            if sku in used_quantities:
                new_qty = row['quantity'] - used_quantities[sku]
                
                if new_qty <= 0:
                    indices_to_drop.append(idx)
                    self.logger.debug(f"  Товар {sku} полностью использован")
                else:
                    self.remaining_products.at[idx, 'quantity'] = new_qty
                    # Пересчитываем total для остатка
                    self.remaining_products.at[idx, 'total'] = new_qty * row['price']
        
        if indices_to_drop:
            self.remaining_products.drop(indices_to_drop, inplace=True)
        
        self.remaining_products.reset_index(drop=True, inplace=True)
    
    def process_all(self, output_dir='output'):
        """
        Обработка всех реестров и сохранение результатов.
        """
        # Создаем выходную директорию
        Path(output_dir).mkdir(exist_ok=True)
        
        results = []
        failed_registries = []
        
        self.logger.info(f"\n{'='*60}")
        self.logger.info("НАЧАЛО ОБРАБОТКИ РЕЕСТРОВ")
        self.logger.info(f"{'='*60}")
        
        for idx, registry in self.registry_df.iterrows():
            target = float(registry['amount'])  # Явно преобразуем в float
            description = str(registry['description'])  # Явно преобразуем в строку
            
            # Очищаем описание для имени файла
            safe_description = "".join(c for c in description if c.isalnum() or c in (' ', '-', '_')).rstrip()
            filename = f"{safe_description} - {target:.2f}.xlsx"
            filepath = os.path.join(output_dir, filename)
            
            # Подбираем товары
            matched_df, actual_sum = self.match_registry(target, description, idx)
            
            if matched_df is not None and not matched_df.empty:
                # Подготавливаем DataFrame для сохранения (удаляем служебные колонки)
                output_df = self.prepare_output_dataframe(matched_df.to_dict('records'))
                
                # Сохраняем в Excel
                output_df.to_excel(filepath, index=False)
                self.logger.info(f"  💾 Сохранено: {filename}")
                
                # Статистика
                adjusted_count = len(matched_df[matched_df.get('price_adjusted', False)]) if 'price_adjusted' in matched_df.columns else 0
                
                results.append({
                    'description': description,
                    'target': target,
                    'actual': actual_sum,
                    'diff': actual_sum - target,
                    'file': filename,
                    'items': len(matched_df),
                    'adjusted': adjusted_count
                })
            else:
                failed_registries.append({
                    'description': description,
                    'target': target,
                    'actual': actual_sum,
                    'diff': actual_sum - target
                })
        
        # Итоговая статистика
        self.logger.info(f"\n{'='*60}")
        self.logger.info("ИТОГОВАЯ СТАТИСТИКА")
        self.logger.info(f"{'='*60}")
        self.logger.info(f"Успешно обработано: {len(results)} из {len(self.registry_df)}")
        
        for r in results:
            status = "✅" if abs(r['diff']) <= 0.01 else "⚠️"
            self.logger.info(f"{status} {r['description']}")
            self.logger.info(f"   Сумма: {r['target']:.2f} -> {r['actual']:.2f} (разница: {r['diff']:+.2f})")
            self.logger.info(f"   Позиций: {r['items']}, с коррекцией цены: {r['adjusted']}")
        
        if failed_registries:
            self.logger.warning(f"\n{'='*60}")
            self.logger.warning("ПРОБЛЕМНЫЕ РЕЕСТРЫ")
            self.logger.warning(f"{'='*60}")
            for f in failed_registries:
                self.logger.warning(f"❌ {f['description']}")
                self.logger.warning(f"   Цель: {f['target']:.2f}, получено: {f['actual']:.2f}")
        
        # Остаток товаров
        self.logger.info(f"\n{'='*60}")
        self.logger.info("ОСТАТОК ТОВАРОВ")
        self.logger.info(f"{'='*60}")
        self.logger.info(f"Позиций: {len(self.remaining_products)}")
        self.logger.info(f"На сумму: {self.remaining_products['total'].sum():.2f}")
        
        # Сохраняем остаток
        if len(self.remaining_products) > 0:
            remainder_file = os.path.join(output_dir, "остаток_товаров.xlsx")
            # Для остатка тоже подготавливаем DataFrame через тот же метод
            remainder_df = self.prepare_output_dataframe(self.remaining_products.to_dict('records'))
            remainder_df.to_excel(remainder_file, index=False)
            self.logger.info(f"Остаток сохранен в: остаток_товаров.xlsx")
        
        # Краткий итог в консоль
        print(f"\n{'='*50}")
        print(f"ОБРАБОТКА ЗАВЕРШЕНА!")
        print(f"{'='*50}")
        print(f"✅ Успешно: {len(results)} из {len(self.registry_df)}")
        print(f"📁 Результаты в папке: {output_dir}")
        print(f"📝 Подробный лог в папке: logs")
        
        return results, failed_registries


# Пример использования
if __name__ == "__main__":
    try:
        # Создаем экземпляр сопоставителя
        matcher = ProductMatcher(
            products_file='ассортимент.xlsx',
            registry_file='реестр.xlsx',
            price_adjustment_limit=0.02
        )
        
        # Загружаем данные
        matcher.load_data()
        
        # Обрабатываем все реестры
        results, failed = matcher.process_all(output_dir='подобранные_товары')
        
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()
