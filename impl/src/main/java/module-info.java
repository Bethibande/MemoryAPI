module memory.impl {

    requires java.base;
    requires jdk.incubator.foreign;

    requires org.jetbrains.annotations;

    exports com.bethibande.memory.impl to MemoryAccess;

}