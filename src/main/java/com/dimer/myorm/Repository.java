package com.dimer.myorm;

import com.dimer.myorm.annotations.*;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.Date;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

public class Repository<T extends Entity<I>, I> implements RepositoryOperation<T, I> {

    private static final boolean LOG_SQL = true;

    private final Connection connection;

    private final Class<T> type;
    private final Map<String, Column> fields = new HashMap<>();
    private Field id;
    private String idName;
    private final int fieldsNumber;

    private final String table;
    private final String select;
    private final String selectAll;
    private final String insert;
    private final String update;
    private final String delete;
    private final String exists;

    private PreparedStatement psSelect;
    private PreparedStatement psSelectAll;
    private PreparedStatement psInsert;
    private PreparedStatement psUpdate;
    private PreparedStatement psDelete;
    private PreparedStatement psExists;

    private final Map<String, BiConsumer<T, Method>> proxyConsumers = new HashMap<>();

    public static <E extends Entity<I>, I> Repository<E, I> of(final Class<E> type) {
        return new Repository<>(type);
    }

    public static String camelToSnake(String str) {
        final String regex = "([a-z])([A-Z]+)";
        final String replacement = "$1_$2";

        return str.replaceAll(regex, replacement).toLowerCase();
    }

    public Repository(Class<T> type) {
        this(type, ConnectionFactory.getConnection());
    }

    public Repository(final Class<T> type, final Connection connection) {
        this.connection = connection;
        this.type = type;
        this.table = getTableName();
        this.fieldsNumber = mapFields(type.getDeclaredFields());

        this.selectAll = "select * from " + table;
        this.select = "select * from " + table + " where id = ? limit 1";
        this.insert = this.generateInsertStatement();
        this.update = this.generateUpdateStatement();
        this.delete = "delete from " + table + " where id = ?";
        this.exists = "select exists (select id from " + table + " where id = ?)";
        this.prepareStatements();
    }

    private ColumnType getColumnType(final Field field) {
        if (isOneToOne(field)) return ColumnType.ONE_TO_ONE;
        if (isOneToMany(field)) return ColumnType.ONE_TO_MANY;
        if (isManyToOne(field)) return  ColumnType.MANY_TO_ONE;
        return ColumnType.SIMPLE;
    }

    private int mapFields(final Field[] fields) {
        int columnNumber = 0;

        for (final Field field : fields) {
            field.setAccessible(true);

            if (field.isAnnotationPresent(Id.class)) {
                this.id = field;
                this.idName = getFieldName(field);
            } else {
                final String fieldName = getFieldName(field);
                final ColumnType columnType = getColumnType(field);

                this.fields.put(fieldName, new Column(field, columnType));

                if (columnType.isNotOneToMany()) {
                    columnNumber++;
                }
            }
        }

        return columnNumber;
    }

    private void prepareStatements() {
        try {
            final String[] generatedValues = new String[]{ "id" };
            this.psSelectAll = connection.prepareStatement(selectAll);
            this.psSelect = connection.prepareStatement(select);
            this.psInsert = connection.prepareStatement(insert, generatedValues);
            this.psUpdate = connection.prepareStatement(update);
            this.psDelete = connection.prepareStatement(delete);
            this.psExists = connection.prepareStatement(exists);
        } catch (SQLException e) {
            throw new RuntimeException("Error while generate sql statements", e);
        }
    }

    private String generateInsertStatement() {
        final List<String> fieldList = new LinkedList<>();
        final List<String> valueList = new LinkedList<>();

        for (final Map.Entry<String, Column> columnEntry : fields.entrySet()) {
            if (!columnEntry.getValue().isOneToMany()) {
                fieldList.add(columnEntry.getKey());
                valueList.add("?");
            }
        }

        final String fields = String.join(",", fieldList);
        final String values = String.join(",", valueList);

        return String.format("insert into %s (%s) values (%s)", table, fields, values);
    }

    private String generateUpdateStatement() {
        final StringBuilder update = new StringBuilder();
        final List<String> statements = new LinkedList<>();

        update.append("update ").append(table).append(" set ");

        for (final Map.Entry<String, Column> columnEntry : fields.entrySet()) {
            if (!columnEntry.getValue().isOneToMany()) {
                statements.add(columnEntry.getKey() + " = ?");
            }
        }

        update.append(String.join(",", statements));

        return update.append(" where id = ?").toString();
    }

    private String getTableName() {
        final String tableName = camelToSnake(type.getSimpleName());

        if (type.isAnnotationPresent(Schema.class)) {
            final String schema = type.getAnnotation(Schema.class).value();
            return schema + "." + tableName;
        }

        return tableName;
    }

    private String getFieldName(Field field) {
        if (isOneToOne(field)) {
            return field.getAnnotation(OneToOne.class).column();
        } else if (isManyToOne(field)) {
            return field.getAnnotation(ManyToOne.class).column();
        } else if (isOneToMany(field)) {
            return field.getAnnotation(OneToMany.class).column();
        }

        return camelToSnake(field.getName());
    }

    private T getProxyInstance() {
        final Enhancer enhancer = new Enhancer();
        enhancer.setClassLoader(type.getClassLoader());
        enhancer.setSuperclass(type);
        enhancer.setCallback((MethodInterceptor) this::executeProxyConsumers);
        return (T) enhancer.create();
    }

    private Object executeProxyConsumers(final Object entity, final Method method, final Object[] args, MethodProxy proxy) {
        try {
            final String methodName = method.getName();

            final BiConsumer<T, Method> consumer = proxyConsumers.get(methodName);

            if (nonNull(consumer)) {
                consumer.accept((T) entity, method);
            }

            return proxy.invokeSuper(entity, args);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    public ResultSet executeQuery(final PreparedStatement statement) throws SQLException {
        if (LOG_SQL) {
            System.out.printf("SQL: %s\n", statement);
        }
        return statement.executeQuery();
    }

    public int executeUpdate(final PreparedStatement statement) throws SQLException {
        if (LOG_SQL) {
            System.out.printf("SQL: %s\n", statement);
        }
        return statement.executeUpdate();
    }

    private T mapperEntity(ResultSet result) {
        try {
            final T entity = getProxyInstance();

            mapIdProperty(entity, result);

            for (final Map.Entry<String, Column> fieldEntry : fields.entrySet()) {
                final Column column = fieldEntry.getValue();
                final String name = fieldEntry.getKey();

                if (column.isRelation()) {
                    mapRelation(entity, name, column, result);
                } else {
                    mapProperty(entity, name, column.get(), result);
                }
            }

            return entity;
        } catch (Exception e) {
            throw new RuntimeException("Error on map entity", e);
        }
    }

    private void createLazyLoadingProxy(final Column column, final Runnable runnable) {
        proxyConsumers.put(getGetterName(column.get()), (entity, method) -> {
            Object result = null;

            try {
                final Field field = type.getDeclaredField(getFieldName(method));
                field.setAccessible(true);
                result = field.get(entity);
            } catch (Exception e) {
                e.printStackTrace();
            }

            if (isNull(result)) {
                runnable.run();
            }
        });
    }

    private String getGetterName(final Field field) {
        String name = field.getName();
        name = name.substring(0, 1).toUpperCase() + name.substring(1);
        return "get" + name;
    }

    private String getFieldName(final Method getter) {
        String name = getter.getName().replace("get", "");
        return name.substring(0, 1).toLowerCase() + name.substring(1);
    }

    private void mapRelation(T entity, String name, Column column, ResultSet result) {
        if (column.isOneToOne() || column.isManyToOne()) {
            if (column.isLazyLoading()) {
                createLazyLoadingProxy(column, () -> mapSingleEntityRelation(entity, name, column.get(), result));
            } else {
                mapSingleEntityRelation(entity, name, column.get(), result);
            }
        } else if (column.isOneToMany()) {
            if (column.isLazyLoading()) {
                createLazyLoadingProxy(column, () -> mapOneToManyRelation(entity, name, column.get(), result));
            } else {
                mapOneToManyRelation(entity, name, column.get(), result);
            }
        }
    }


    private Object saveRelation(T entity, Column column, boolean update) {
        if (column.isOneToOne()) {
            return saveOneRelation(entity, column.get());
        } else if (column.isOneToMany()) {
            saveOneToManyRelation(entity, column.get());
            return null;
        } else if (column.isManyToOne()) {
            return getManyToOneRelationId(entity, column.get());
        }

        return null;
    }

    private Object getManyToOneRelationId(T entity, Field field) {
        try {
            final Class<? extends Entity<?>> fieldType = (Class<? extends Entity<?>>) field.getType();
            final Object relatedEntity = field.get(entity);

            if (nonNull(relatedEntity)) {
                final Object id = castToEntity(relatedEntity).getId();

                final Repository<? extends Entity<Object>, Object> repository = new Repository<>(
                        (Class<Entity<Object>>) fieldType, connection);

                final Entity<?> savedEntity;

                if (repository.exists(id)) {
                    savedEntity = repository.find(id);
                } else {
                    savedEntity = repository.save(castToEntity(relatedEntity));
                }

                field.set(entity, savedEntity);

                return nonNull(savedEntity) ? savedEntity.getId() : null;
            }

            return null;
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Error on map many to one relation id", e);
        }
    }

    private Object saveOneRelation(T entity, Field field) {
        final Class<? extends Entity<?>> fieldType = (Class<? extends Entity<?>>) field.getType();

        if (!Entity.class.isAssignableFrom(fieldType)) {
            throw new RuntimeException("Field annotated with one to one relation is not an Entity.");
        }

        try {
            final Entity<?> relatedEntity = (Entity<?>) field.get(entity);

            if (isNull(relatedEntity)) {
                return null;
            }

            final Repository<? extends Entity<Object>, Object> repository = new Repository<>((Class<Entity<Object>>) fieldType, connection);

            final Entity<?> savedEntity;

            if (nonNull(relatedEntity.getId())) {
                savedEntity = repository.find(relatedEntity.getId());
            } else {
                savedEntity = repository.save(castToEntity(relatedEntity));
            }

            field.set(entity, savedEntity);

            return nonNull(savedEntity) ? savedEntity.getId() : null;

        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

        return null;
    }

    private void saveOneToManyRelation(T entity, Field field) {
        final Class<?> fieldType = field.getType();

        if (!Collection.class.isAssignableFrom(fieldType)) {
            throw new RuntimeException("Field annotated with one to many relation is not a collection.");
        }

        try {
            final Object list = field.get(entity);

            if (isNull(list)) {
                return;
            }

            final Repository<? extends Entity<?>, ?> repository = new Repository<>((Class<Entity<Object>>) fieldType, connection);
            final List<? extends Entity<?>> entities = repository.saveOrUpdateAll(castToEntitiesList(list));

            field.set(entity, entities);

        } catch (IllegalAccessException e) {
            throw new RuntimeException("Error on save one to many relation", e);
        }
    }

    private <E extends Entity<?>> E castToEntity(Object value) {
        return (E) value;
    }

    private <E extends Entity<?>> List<E> castToEntitiesList(Object value) {
        return (List<E>) value;
    }

    private void mapOneToManyRelation(T entity, String name, Field field, ResultSet result) {
        final Class<?> fieldType = field.getType();

        if (!Collection.class.isAssignableFrom(fieldType)) {
            throw new RuntimeException("Field annotated with one to many relation is not a collection.");
        }

        try {
            final Repository<? extends Entity<?>, ?> repository = new Repository<>(getListType(field), connection);
            final List<? extends Entity<?>> entities = repository.findAllByColumn(name, entity.getId());

            field.set(entity, entities);

        } catch (IllegalAccessException e) {
            throw new RuntimeException("Error on map one to many relation", e);
        }
    }

    private void mapSingleEntityRelation(T entity, String name, Field field, ResultSet result) {
        final Class<?> fieldType = field.getType();

        if (!Entity.class.isAssignableFrom(fieldType)) {
            throw new RuntimeException("Field annotated with one to one relation is not an Entity.");
        }

        try {
            final Object id = getValueFromResultSet(name, getPrimaryKeyType(fieldType) , result);
            final Repository<? extends Entity<Object>, Object> repository = new Repository<>(
                    (Class<Entity<Object>>) fieldType, connection);

            final Object object = repository.find(id);

            field.set(entity, object);

        } catch (SQLException | IllegalAccessException e) {
            throw new RuntimeException("Error on map one to one relation", e);
        }
    }

    private Class<?> getPrimaryKeyType(Class<?> fieldType) {
        final Type[] genericInterfaces = fieldType.getGenericInterfaces();

        final Optional<ParameterizedType> entityType = Arrays.stream(genericInterfaces)
                .filter(type -> type instanceof ParameterizedType &&
                        ((ParameterizedType) type).getRawType().equals(Entity.class))
                .findFirst()
                .map(ParameterizedType.class::cast);

        if (entityType.isEmpty()) {
            throw new RuntimeException("Can't get the type of related entity primary key.");
        }
        return (Class<?>) entityType.get().getActualTypeArguments()[0];
    }

    private Class<? extends Entity<Object>> getListType(Field field) {
        final Type[] types = ((ParameterizedType) field.getGenericType()).getActualTypeArguments();

        if (types.length != 1) {
            throw new RuntimeException("Can't get the type of related entity primary key.");
        }

        return (Class<? extends Entity<Object>>) types[0];
    }

    private boolean isOneToOne(Field field) {
        return field.isAnnotationPresent(OneToOne.class);
    }

    private boolean isOneToMany(Field field) {
        return field.isAnnotationPresent(OneToMany.class);
    }

    private boolean isManyToOne(Field field) {
        return field.isAnnotationPresent(ManyToOne.class);
    }

    private void mapIdProperty(final T entity, final ResultSet result) throws SQLException, IllegalAccessException {
        mapProperty(entity, idName, id, result);
    }
    private void mapProperty(final T entity, final String name, final Field field, final ResultSet result)
            throws SQLException, IllegalAccessException {
        final Class<?> type = field.getType();
        field.set(entity, getValueFromResultSet(name, type, result));
    }

    public static <E extends Enum<?>> E castEnum(Object field) {
        return (E) field;
    }

    private Object getValueFromResultSet(String columnName, Class<?> columnType, ResultSet result) throws SQLException {
        if (columnType.equals(String.class)) {
            return result.getString(columnName);
        } else if (columnType.equals(Integer.class)) {
            return result.getInt(columnName);
        } else if (columnType.equals(LocalDate.class)) {
            final Date date = result.getDate(columnName);
            return isNull(date) ? null : date.toLocalDate();
        } else if (columnType.equals(LocalDateTime.class)) {
            final Timestamp timestamp = result.getTimestamp(columnName);
            return isNull(timestamp) ? null : timestamp.toLocalDateTime();
        } else if (columnType.equals(Float.class)) {
            return result.getFloat(columnName);
        } else if (columnType.equals(Double.class)) {
            return result.getDouble(columnName);
        } else if (columnType.equals(Boolean.class)) {
            return result.getBoolean(columnName);
        } else if (columnType.equals(Long.class)) {
            return result.getLong(columnName);
        } else if (columnType.isEnum()) {
            final int ordinal = result.getInt(columnName);
            return Arrays.stream(columnType.getEnumConstants())
                    .filter(e -> castEnum(e).ordinal() == ordinal)
                    .findFirst()
                    .orElse(null);
        }

        return null;
    }

    public static void setValueIntoStatement(PreparedStatement ps, int columnIndex, Object value) throws SQLException {
        if (isNull(value)) {
            ps.setNull(columnIndex, Types.NULL);
        } else if (value instanceof String) {
            ps.setString(columnIndex, value.toString());
        } else if (value instanceof Integer) {
            ps.setInt(columnIndex, (Integer) value);
        } else if (value instanceof LocalDate) {
            ps.setDate(columnIndex, Date.valueOf(((LocalDate) value)));
        } else if (value instanceof LocalDateTime) {
            ps.setTimestamp(columnIndex, Timestamp.valueOf(((LocalDateTime) value)));
        } else if (value instanceof Float) {
            ps.setFloat(columnIndex, (Float) value);
        } else if (value instanceof Double) {
            ps.setDouble(columnIndex, (Double) value);
        } else if (value instanceof Boolean) {
            ps.setBoolean(columnIndex, (Boolean) value);
        } else if (value instanceof Long) {
            ps.setLong(columnIndex, (Long) value);
        } else if (value.getClass().isEnum()) {
            ps.setInt(columnIndex, castEnum(value).ordinal());
        }
    }

    private void verifyFieldExistence(final String field) {
        if (!fields.containsKey(field)) {
            throw new RuntimeException("The field " + field + " does not exists.");
        }
    }

    @Override
    public List<T> findAll() {
        try {
            final List<T> resultList = new ArrayList<>();
            final ResultSet queryResults = executeQuery(psSelectAll);

            while (queryResults.next()) {
                T entity = mapperEntity(queryResults);
                resultList.add(entity);
            }

            return resultList;
        } catch (SQLException e) {
            throw new RuntimeException("Error on find results of " + type.getSimpleName(), e);
        }
    }

    @Override
    public T find(I id) {
        try {
            psSelect.clearParameters();

            setValueIntoStatement(psSelect, 1, id);

            final ResultSet result = executeQuery(psSelect);
            return result.next() ? mapperEntity(result) : null;
        } catch (SQLException e) {
            throw new RuntimeException("Error on find unique result of " + type.getSimpleName(), e);
        }
    }

    @Override
    public T save(T entity) {
        int index = 1;

        try {
            psInsert.clearParameters();

            for (final Map.Entry<String, Column> fieldEntry : fields.entrySet()) {
                final Column column = fieldEntry.getValue();
                final Field field = column.get();

                final Object value;

                if (column.isRelation()) {
                    value = saveRelation(entity, column, false);

                    if (column.isOneToMany()) {
                        continue;
                    }
                } else {
                    value = field.get(entity);
                }

                setValueIntoStatement(psInsert, index, value);
                index++;
            }

            final int affectedRows = executeUpdate(psInsert);

            if (affectedRows == 1) {
                final ResultSet generatedKeys = psInsert.getGeneratedKeys();

                if (generatedKeys.next()) {
                    mapIdProperty(entity, generatedKeys);
                    return entity;
                }
            }

            return null;

        } catch (Exception e) {
            throw new RuntimeException("Error while save entity", e);
        }
    }

    @Override
    public List<T> saveAll(List<T> entities) {
        return entities.stream().map(this::save).collect(Collectors.toList());
    }

    public T saveOrUpdate(T entity) {
        if (isNull(entity)) {
            return null;
        }

        if (nonNull(entity.getId()) && exists(entity.getId())) {
            return this.update(entity);
        }

        return this.save(entity);
    }

    private List<T> saveOrUpdateAll(List<T> entities) {
        return entities.stream().map(this::saveOrUpdate).collect(Collectors.toList());
    }

    @Override
    public T update(T entity) {
        int index = 1;

        try {
            psUpdate.clearParameters();

            for (final Map.Entry<String, Column> columnEntry : fields.entrySet()) {
                final Column column = columnEntry.getValue();
                final Object value;

                if (column.isRelation()) {
                    value = saveRelation(entity, column, true);

                    if (column.isOneToMany()) {
                        continue;
                    }
                } else {
                    value = column.get().get(entity);
                }

                setValueIntoStatement(psUpdate, index, value);
                index++;
            }

            setValueIntoStatement(psUpdate, fieldsNumber + 1, entity.getId());

            final int affectedRows = executeUpdate(psUpdate);

            if (affectedRows == 1) {
                return entity;
            }
            return null;
        } catch (Exception e) {
            throw new RuntimeException("Error on update entity " + type.getSimpleName(), e);
        }
    }

    @Override
    public List<T> updateAll(List<T> entities) {
        return entities.stream().map(this::update).collect(Collectors.toList());
    }

    @Override
    public boolean delete(T entity) {
        return deleteById(entity.getId());
    }

    @Override
    public boolean deleteById(I id) {
        try {
            psDelete.clearParameters();

            setValueIntoStatement(psDelete, 1, id);

            return executeUpdate(psDelete) > 0;
        } catch (SQLException e) {
            throw new RuntimeException("Error on delete entity " + type.getSimpleName(), e);
        }
    }

    @Override
    public T findByColumn(String column, Object value) {
        try {
            verifyFieldExistence(column);

            final String selectByColumn = "select * from " + table + " where " + column + " = ? limit 1";
            final PreparedStatement preparedStatement = connection.prepareStatement(selectByColumn);

            setValueIntoStatement(preparedStatement, 1, value);

            final ResultSet result = executeQuery(preparedStatement);

            return result.next() ? mapperEntity(result) : null;
        } catch (SQLException e) {
            throw new RuntimeException("Error on find results of " + type.getSimpleName(), e);
        }
    }

    @Override
    public List<T> findAllByColumn(String column, Object value) {
        try {
            verifyFieldExistence(column);

            final String selectByColumn = "select * from " + table + " where " + column + " = ?";
            final PreparedStatement preparedStatement = connection.prepareStatement(selectByColumn);

            setValueIntoStatement(preparedStatement, 1, value);

            final ResultSet result = executeQuery(preparedStatement);
            final List<T> resultList = new ArrayList<>();

            while (result.next()) {
                T entity = mapperEntity(result);
                resultList.add(entity);
            }

            return resultList;
        } catch (SQLException e) {
            throw new RuntimeException("Error on find results of " + type.getSimpleName(), e);
        }
    }

    @Override
    public boolean exists(I id) {
        if (isNull(id)) return false;

        try {
            psExists.clearParameters();

            setValueIntoStatement(psExists, 1, id);

            final ResultSet result = executeQuery(psExists);

            return result.next() && result.getBoolean(1);
        } catch (SQLException e) {
            return false;
        }
    }

    private enum ColumnType {
        SIMPLE, ONE_TO_ONE, ONE_TO_MANY, MANY_TO_ONE;

        public boolean isNotOneToMany() {
            return this != ONE_TO_MANY;
        }
    }

    private static class Column {
        private final Field field;
        private final ColumnType columnType;

        public Column(final Field field, final ColumnType columnType) {
            this.field = field;
            this.columnType = columnType;
        }

        public Field get() {
            return field;
        }

        public boolean isRelation() {
            return columnType != ColumnType.SIMPLE;
        }

        public boolean isOneToOne() {
            return columnType == ColumnType.ONE_TO_ONE;
        }

        public boolean isOneToMany() {
            return columnType == ColumnType.ONE_TO_MANY;
        }

        public boolean isManyToOne() {
            return columnType == ColumnType.MANY_TO_ONE;
        }

        private FetchType getFetchType() {
            if (isOneToMany()) {
                return field.getAnnotation(OneToMany.class).fetchType();
            }

            if (isOneToOne()) {
                return field.getAnnotation(OneToOne.class).fetchType();
            }

            if (isManyToOne()) {
                return field.getAnnotation(ManyToOne.class).fetchType();
            }

            return null;
        }
        public boolean isLazyLoading() {
            return isRelation() && FetchType.LAZY.equals(getFetchType());
        }
    }
}
